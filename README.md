# Retail Data Pipeline

A complete end-to-end retail data pipeline powered by Apache Airflow, dbt, and Google Cloud Platform. This project demonstrates a modern data engineering workflow from synthetic data generation through transformations to analytics-ready tables in BigQuery.

## Architecture

```
Generate Sample Data → Upload to GCS → Load to BigQuery → Deduplicate → dbt Transform
                                                                        ├─ Bronze (Clean)
                                                                        ├─ Silver (Enrich)
                                                                        └─ Gold (Aggregate)
```

## Features

- **Single DAG Pipeline**: One unified DAG (`retail_pipeline`) orchestrates the entire workflow
- **Synthetic Data Generation**: Built-in Faker-based data generator with intentional duplicates for testing
- **Automatic Deduplication**: Removes duplicate rows after loading to BigQuery
- **Medallion Architecture**: Bronze → Silver → Gold layers using dbt
- **Infrastructure as Code**: Terraform modules for GCP provisioning
- **Dockerized Airflow**: Local development environment with Docker Compose

## Repository Structure

```
retail-data-pipeline/
├── airflow/
│   ├── dags/
│   │   └── retail_pipeline.py      # Main unified DAG
│   ├── docker-compose.yaml         # Airflow services
│   ├── keys/                       # GCP service account keys (gitignored)
│   └── .env                        # Airflow configuration (gitignored)
├── dbt/retail/
│   ├── models/
│   │   ├── bronze/                 # Raw data cleaning
│   │   ├── silver/                 # Enriched tables
│   │   └── gold/                   # Business metrics
│   ├── macros/                     # Custom dbt macros
│   └── profiles.yml                # dbt connection config
├── infra/                          # Terraform GCP infrastructure
└── data_samples/                   # CSV files (auto-generated)
```

## Prerequisites

- **Docker Desktop** 4.24+ with 4 CPUs / 6 GB RAM
- **Terraform** ≥ 1.5 (optional, for infrastructure provisioning)
- **Python** 3.12+ with pip (for local dbt development)
- **Google Cloud Project** with billing enabled

## Quick Start

### 1. Provision GCP Infrastructure

```bash
cd infra
terraform init
terraform apply -var="project_id=YOUR_PROJECT_ID"
```

Save the generated service account key as `airflow/keys/airflow-sa.json`.

### 2. Configure Airflow

Create `airflow/.env`:

```env
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# GCP Configuration
GCP_PROJECT=retail-data-pipeline-dev
GCS_BUCKET=retail-data-pipeline-raw
GCS_TARGET_BUCKET=retail-data-pipeline-raw
BQ_DATASET=retail_raw
BQ_LOCATION=asia-southeast1
BQ_TABLE_LIST=users,products,orders,order_items,sessions

# dbt Configuration
DBT_PROJECT_ID=retail-data-pipeline-dev
DBT_RAW_DATASET=retail_raw
DBT_DATASET=retail_prod
DBT_LOCATION=asia-southeast1
DBT_PROJECT_DIR=/opt/airflow/dbt/retail
DBT_TARGET_DATASET=retail_prod
```

### 3. Start Airflow

```bash
cd airflow
docker-compose up -d
```

Access the UI at http://localhost:8080 (credentials: `airflow` / `airflow`)

### 4. Create GCP Connection

```bash
docker-compose exec airflow-scheduler airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{
    "extra__google_cloud_platform__project": "retail-data-pipeline-dev",
    "extra__google_cloud_platform__key_path": "/opt/airflow/keys/airflow-sa.json"
  }'
```

### 5. Run the Pipeline

Trigger the `retail_pipeline` DAG from the Airflow UI or CLI:

```bash
docker-compose exec airflow-scheduler airflow dags trigger retail_pipeline
```

## Pipeline Stages

### 1. Generate Sample Data
- Creates synthetic retail data (1000 users, 200 products, 5000 orders)
- Intentionally injects ~0.5% duplicates for testing deduplication
- Outputs to `/opt/airflow/data_samples/`

### 2. Ingest to GCS
- Uploads CSV files to GCS bucket
- Organizes by date: `gs://bucket/YYYY-MM-DD/*.csv`

### 3. Load to BigQuery
- Loads all CSVs to `retail_raw` dataset
- Uses explicit schema for `users` table (avoids autodetect issues)
- Autodetects schema for other tables

### 4. Deduplicate
- Runs `SELECT DISTINCT *` on each table
- Removes duplicate rows while preserving all unique data
- Reports duplicates removed per table

### 5. dbt Transformations

**Bronze Layer** (`retail_prod` dataset)
- `bronze__users`: Clean names, normalize phones, parse timestamps
- `bronze__orders`: Parse dates, clean text fields

**Silver Layer**
- `silver__orders_enriched`: Join orders with users, add date dimensions

**Gold Layer**
- `gold__daily_branch_metrics`: Aggregate sales by branch and date

## dbt Local Development

```bash
cd dbt/retail
python -m venv .venv && source .venv/bin/activate
pip install dbt-bigquery

export DBT_PROJECT_ID=retail-data-pipeline-dev
export DBT_DATASET=retail_prod
export DBT_RAW_DATASET=retail_raw
export GOOGLE_APPLICATION_CREDENTIALS=../../airflow/keys/airflow-sa.json

dbt deps
dbt run --select path:models/bronze
dbt test --select path:models/bronze
dbt run --select path:models/silver path:models/gold
dbt test
```

## Useful Commands

### Airflow
```bash
# List DAGs
docker-compose exec airflow-scheduler airflow dags list

# Trigger DAG
docker-compose exec airflow-scheduler airflow dags trigger retail_pipeline

# View logs
docker-compose logs -f airflow-scheduler

# Restart services
docker-compose restart

# Clean up
docker-compose down -v
```

### dbt (from Airflow container)
```bash
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt/retail && dbt run"
```

## Data Quality

The pipeline includes built-in data quality checks:

- **Uniqueness tests**: Warn when duplicates found (should be 0 after deduplication)
- **Not-null tests**: Warn when critical fields are null
- **Freshness tests**: Ensure source data is recent
- **Custom tests**: dbt_expectations package for advanced validations

Tests are configured to **warn** rather than **fail** to allow pipeline to continue while flagging issues.

## Troubleshooting

**DAG not appearing**
```bash
docker-compose restart airflow-dag-processor
docker-compose exec airflow-scheduler airflow dags list
```

**Connection error**
```bash
# Verify credentials
docker-compose exec airflow-scheduler ls -la /opt/airflow/keys/
# Test connection
docker-compose exec airflow-scheduler airflow connections test google_cloud_default
```

**dbt errors**
```bash
# Check environment variables
docker-compose exec airflow-scheduler env | grep DBT
# Run dbt debug
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt/retail && dbt debug"
```

**Duplicate dataset names (e.g., retail_raw_retail_prod)**
- This was fixed by adding custom `generate_schema_name` macro
- Ensures dbt uses only `retail_prod` as target dataset

## Architecture Decisions

- **Single DAG**: Consolidates all steps for easier orchestration and monitoring
- **Deduplication**: Performed at load time rather than in dbt for performance
- **Warning-only tests**: Allows pipeline to complete while flagging data quality issues
- **Explicit schema for users**: Avoids BigQuery autodetect issues with certain CSV formats
- **Medallion layers**: Bronze (clean) → Silver (enrich) → Gold (aggregate)

## Next Steps

- [ ] Add incremental models in dbt
- [ ] Implement CDC (Change Data Capture) from source systems
- [ ] Add data validation with Great Expectations
- [ ] Set up CI/CD pipeline with GitHub Actions
- [ ] Add monitoring and alerting (e.g., Slack notifications)
- [ ] Create Looker/Tableau dashboards

## Contributing

Contributions welcome! Please open an issue or PR with improvements.

## License

MIT
