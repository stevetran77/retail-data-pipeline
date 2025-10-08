# Retail dbt Project

This directory contains the dbt project that shapes the CSV extracts landed by Airflow into analytics-ready BigQuery tables. The starter project includes:
- BigQuery profile that reads credentials and dataset names from environment variables.
- Bronze models that normalise raw orders and users data.
- Silver model that enriches orders with customer attributes.
- Gold aggregate that reports daily revenue metrics by branch.
- Source and model tests wired through `dbt_utils` and core dbt test macros.

## 1. Install dependencies

A `pyproject.toml` with `dbt-bigquery` already lives at the repository root. From the repo root you can install everything with [uv](https://github.com/astral-sh/uv) or your preferred tool:

```powershell
uv sync --python 3.13
```

Activate the environment for subsequent commands:

```powershell
.\.venv\Scripts\Activate.ps1
```

(Or create a virtual environment manually and `pip install dbt-bigquery==1.8.2`.)

## 2. Prepare credentials and environment variables

dbt will look for the profile in this folder, so set `DBT_PROFILES_DIR` and export the connection details that match your GCP project:

```powershell
Set-Location dbt/retail
$env:DBT_PROFILES_DIR = (Get-Location)
$env:DBT_PROJECT_ID = "retail-data-pipeline-dev"      # update to your project id
$env:DBT_DATASET = "retail_prod"                      # dataset created by Terraform
$env:DBT_RAW_DATASET = "retail_raw"                   # dataset that stores raw external tables
$env:DBT_LOCATION = "asia-southeast1"                 # region of your BigQuery resources
$env:GOOGLE_APPLICATION_CREDENTIALS = "..\..\airflow\keys\airflow-sa.json"
```

If you prefer to rely on `gcloud auth application-default login`, change `method` in `profiles.yml` to `oauth` and drop the `keyfile` property.

Make sure the service account in `GOOGLE_APPLICATION_CREDENTIALS` has at least `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the project.

## 3. Stage raw tables

The models expect the raw CSVs to be available in BigQuery under the dataset referenced by `DBT_RAW_DATASET` with table names `orders`, `order_items`, `users`, `products`, and `sessions`. You can populate them by loading the sample CSVs:

```powershell
$bqDataset = "$env:DBT_PROJECT_ID:$env:DBT_RAW_DATASET"
foreach ($file in Get-ChildItem ..\..\data_samples\*.csv) {
  $table = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
  bq load --replace --autodetect --source_format=CSV "$bqDataset.$table" $file.FullName
}
```

Feel free to adapt this step if you ingest the data with another pipeline.

## 4. Run dbt

Once the raw tables exist:

```powershell
dbt deps
dbt debug
dbt build
```

The run will create models in the `DBT_DATASET` dataset with table names matching the filenames (for example `retail_prod.bronze__orders`). `dbt build` also executes the schema tests defined in the project.

## 5. Extend the project

- Add more bronze models to clean remaining raw entities (`order_items`, `products`, `sessions`).
- Enrich silver models with web analytics and product data for attribution analyses.
- Layer on gold marts (e.g. customer lifetime value, cohort retention dashboards).
- Wire this project into CI/CD by calling `dbt build` from GitHub Actions before deployments.


## 6. Airflow orchestration

Trigger the `dbt_pipeline` DAG after `load_gcs_to_bq` finishes to execute `dbt deps`, bronze cleanup, and silver/gold transforms in sequence. The DAG reads environment defaults from `airflow/.env`, so update `DBT_TARGET_DATASET`, `DBT_PROJECT_DIR`, or related variables there if you need to point at a different dataset.
