# Retail Data Pipeline

A local-first playground for building a modern analytics stack on top of Google Cloud. The project pairs an Apache Airflow instance (running in Docker) with dbt, synthetic retail data, and Terraform modules so you can practice ingesting raw CSV files into Google Cloud Storage and shape them for downstream analytics in BigQuery.

## Features
- Sample DAG that uploads CSV drops from `data_samples/` into a raw GCS bucket using a service account.
- Synthetic retail dataset generator with deliberate data-quality issues for testing cleansing logic.
- Terraform modules that provision the minimum GCP resources (bucket, dataset, service account) for the stack.
- Skeleton dbt project ready to compile models against BigQuery once the raw layer is populated.

## Repository Layout
| Path | Description |
| --- | --- |
| `airflow/` | Dockerised Airflow stack (dags, configs, compose file, runtime keys/logs). |
| `data_samples/` | Local CSV inputs; mounted into Airflow containers at `/opt/airflow/data_samples`. |
| `dbt/retail/` | dbt project scaffold (macros, models, seeds, tests). |
| `infra/` | Terraform configuration for bucket, dataset, and service account. |
| `scripts/` | Utility scripts, including sample data generation. |

## Prerequisites
- Docker Desktop 4.24+ (Compose v2) with at least 4 CPUs / 6 GB RAM allocated.
- Terraform ≥ 1.5 if you want to provision the GCP infrastructure from `infra/`.
- Python 3.13 (or `uv`, `pipx`, etc.) for running helper scripts locally.
- A Google Cloud project with billing enabled and permissions to create buckets, datasets, and service accounts.

## 1. Provision Cloud Resources (optional but recommended)
1. Authenticate with gcloud and set your project: `gcloud config set project <PROJECT_ID>`.
2. Update `infra/variables.tf` if you want a different region or dataset name.
3. Initialise and apply Terraform:
   ```bash
   cd infra
   terraform init
   terraform apply -var="project_id=<PROJECT_ID>"
   ```
   Note the outputs:
   - `bucket_name` (e.g. `retail-data-pipeline-dev-retail-raw`)
   - `airflow_sa_email`
   - `airflow_sa_key_base64`
4. Store the generated service-account key JSON locally as `airflow/keys/airflow-sa.json` (this file is git-ignored).

If you already have a bucket and service account, place the existing JSON key under `airflow/keys/airflow-sa.json` and ensure it has permissions to write objects to the target bucket.

## 2. Prepare Local Configuration
Create `airflow/.env` with the Airflow user credentials and UID (Linux/mac requires matching UID; on Windows any value works):
```dotenv
AIRFLOW_UID=1000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

Then create/update the Airflow variable file (`airflow/config/variables.json`) or set it later from the UI. At minimum you need the bucket name the DAG should write to:
```json
{
  "GCS_INGEST_BUCKET": "retail-data-pipeline-raw"
}
```
You may also set this via the Airflow UI (`Admin → Variables`) or by running `docker compose run --rm airflow-cli variables set ...`.

## 3. Generate Sample Data
Populate `data_samples/` with fresh CSVs (only `.csv` files are ingested). The helper script produces users, products, orders, order items, and sessions along with intentional dirty records:
```bash
python scripts/gen_sample_date.py
```
The Airflow stack mounts `../data_samples` into `/opt/airflow/data_samples`, so the DAG will see the files immediately.

## 4. Start Airflow Locally
1. Move into the Airflow folder and spin up the stack:
   ```bash
   cd airflow
   docker compose up -d
   ```
2. Wait for the services to start (`docker compose ps`).
3. Access the UI at http://localhost:8080 (defaults: `airflow` / `airflow`).
4. Create the GCP connection if it does not already exist:
   ```bash
   docker compose run --rm airflow-cli connections add \
     google_cloud_default \
     --conn-type google_cloud_platform \
     --conn-extra '{"extra__google_cloud_platform__project": "retail-data-pipeline-dev", "extra__google_cloud_platform__key_path": "/opt/airflow/keys/airflow-sa.json"}'
   ```
   Adjust the project and key path to match your setup.

The compose file already disables tutorial DAGs, forces the UI to English, and mounts:
- `dags/` → `/opt/airflow/dags`
- `../data_samples/` → `/opt/airflow/data_samples`
- `keys/` → `/opt/airflow/keys`

## 5. Run the Ingestion DAG
Trigger the DAG from the UI or run an ad-hoc test:
```bash
cd airflow
docker compose run --rm airflow-cli dags test ingest_data_samples_to_gcs 2024-01-01
```
The DAG performs three steps:
1. Resolve the destination bucket (`Variable GCS_INGEST_BUCKET` or `GCS_TARGET_BUCKET` env var).
2. Discover only `.csv` files in `/opt/airflow/data_samples`.
3. Upload each file with a dynamic task map using the connection `google_cloud_default`. Objects are written under `<yyyy-mm-dd>/<filename>` where the date is the UTC execution time.

## 6. dbt Project and Orchestration
The `dbt/retail` directory contains the models that shape raw extracts into analytics layers. You can run them locally for development or orchestrate them from Airflow once the raw tables are populated.

**Local runs**
```bash
cd dbt/retail
python -m venv .venv && source .venv/bin/activate  # or use uv/pipx
pip install dbt-bigquery==1.8.2
export DBT_PROJECT_ID=retail-data-pipeline-dev
export DBT_DATASET=retail_prod
export DBT_RAW_DATASET=retail_raw
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/airflow-sa.json
dbt deps
dbt run --select path:models/bronze
```
Run `dbt run --select path:models/silver path:models/gold` and `dbt test` once you are happy with the bronze layer.

**Airflow runs**
1. Trigger `load_gcs_to_bq` to land the latest CSVs into the raw dataset.
2. Trigger `dbt_pipeline` to execute `dbt deps`, run bronze models, test them, then build the silver and gold layers in the `retail_prod` dataset.
3. Inspect the run logs in the Airflow UI or BigQuery console to verify new tables.

Environment variables such as `DBT_TARGET_DATASET`, `DBT_PROJECT_DIR`, and `DBT_PROFILES_DIR` are configurable via `airflow/.env` or the Compose file if you need to override defaults.

## 7. Useful Commands
- List Airflow variables: `docker compose run --rm airflow-cli variables list`
- Delete a manual DagRun: `docker compose run --rm airflow-cli dags delete <dag-id> --yes`
- Tear down the stack (and volumes): `docker compose down -v`
- Rotate the service-account key: rerun `terraform apply` or use `gcloud iam service-accounts keys create`.

## Troubleshooting
- **UI language not English** – the compose file sets `AIRFLOW__WEBSERVER__DEFAULT_UI_LANGUAGE=en`. If you still see another language, clear browser cache or override the setting via environment variables.
- **`google_cloud_default` not found** – create the connection using the command above or through `Admin → Connections`.
- **`Default credentials were not found`** – confirm the JSON key exists at `/opt/airflow/keys/airflow-sa.json` inside the container and the service account has Storage permissions.
- **DAG uploads files into nested `raw/data_samples/...` prefixes** – adjust the `DEFAULT_OBJECT_PREFIX` constant in `dags/ingest_data_samples_to_gcs.py` if you prefer a different folder layout.
- **Existing `.env` tracked by git** – the repo ignores `airflow/.env`. If your copy was committed previously, run `git rm --cached airflow/.env` once.

## Next Steps
- Build dbt staging and mart models off the data landing in BigQuery.
- Add Airflow DAGs that load the CSVs into BigQuery tables (via the Storage → BigQuery transfer or custom hooks).
- Add data-quality checks (Great Expectations, dbt tests) and notify on failures.
- Integrate CI by running `dags test` and `dbt build` in GitHub Actions.

Happy hacking! If you find issues or want to contribute improvements, feel free to open a PR.
