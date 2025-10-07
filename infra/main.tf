terraform {
  required_version  = ">= 1.5.0"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 5.0" }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Bật API cần thiết (an toàn nếu đã bật trước đó)
resource "google_project_service" "enable_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "serviceusage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])
  project = var.project_id
  service = each.value
}

# Bucket GCS cho raw
resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-retail-raw"
  location                    = var.region
  uniform_bucket_level_access = true
  versioning { enabled = true }
  lifecycle_rule {
    action    { type = "Delete" }
    condition { age = 60 } # xóa object >60 ngày (tùy chỉnh)
  }
  depends_on = [google_project_service.enable_apis]
}

# Dataset BigQuery
resource "google_bigquery_dataset" "core" {
  dataset_id = var.dataset_id
  location   = var.region
  depends_on = [google_project_service.enable_apis]
}

# Service Account cho Airflow/dbt
resource "google_service_account" "airflow" {
  account_id   = "airflow-runner"
  display_name = "Airflow Runner"
}

# Cấp quyền tối thiểu
resource "google_project_iam_member" "bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}
resource "google_project_iam_member" "gcs_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# (Dev) Tạo key để chạy local — KHÔNG commit file key
resource "google_service_account_key" "airflow_key" {
  service_account_id = google_service_account.airflow.name
  keepers = { rotated = timestamp() }
}

output "bucket_name"           { value = google_storage_bucket.raw.name }
output "dataset_id"            { value = google_bigquery_dataset.core.dataset_id }
output "airflow_sa_email"      { value = google_service_account.airflow.email }
output "airflow_sa_key_base64" { value = google_service_account_key.airflow_key.private_key }
