from __future__ import annotations

import mimetypes
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from airflow.sdk import dag, get_current_context, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_ID = os.getenv("GCP_PROJECT")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET", "retail_raw")
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-southeast1")
TABLE_LIST_STR = os.getenv("BQ_TABLE_LIST")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt/retail")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt/retail")

# Ingest configuration
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
BUCKET_VARIABLE_KEY = "GCS_INGEST_BUCKET"
BUCKET_ENV_VAR = "GCS_TARGET_BUCKET"
SOURCE_DIR_ENV_VAR = "DATA_SAMPLES_DIR"
OBJECT_PREFIX_ENV_VAR = "GCS_OBJECT_PREFIX"
DEFAULT_SOURCE_DIR = "/opt/airflow/data_samples"
DEFAULT_OBJECT_PREFIX = ""
UPLOAD_DATE_FORMAT = "%Y-%m-%d"


class ConfigError(RuntimeError):
    """Raised when the DAG configuration is incomplete."""


if not all([PROJECT_ID, GCS_BUCKET, BQ_DATASET, TABLE_LIST_STR]):
    raise ConfigError(
        "Missing required environment variables. "
        "Ensure GCP_PROJECT, GCS_BUCKET, BQ_DATASET, and BQ_TABLE_LIST are set."
    )

TABLE_LIST = [table.strip() for table in TABLE_LIST_STR.split(",") if table.strip()]


# ========== GENERATE SAMPLE DATA FUNCTION ==========
def generate_sample_data():
    """Generate synthetic retail data with Faker."""
    import random
    import numpy as np
    import pandas as pd
    from faker import Faker
    from tqdm import tqdm

    # Config
    OUT_DIR = "/opt/airflow/data_samples"
    SEED = 77
    N_USERS = 1000
    N_PRODUCTS = 200
    N_ORDERS = 5000
    N_SESSIONS = 8000
    START_DATE = "2024-01-01"
    END_DATE = "2025-10-01"
    PCT_DUPLICATES = 0.005

    os.makedirs(OUT_DIR, exist_ok=True)
    random.seed(SEED)
    np.random.seed(SEED)
    fake = Faker("vi_VN")
    Faker.seed(SEED)
    start_dt = datetime.fromisoformat(START_DATE)
    end_dt = datetime.fromisoformat(END_DATE)

    def random_timestamp_between(start_dt, end_dt):
        delta = end_dt - start_dt
        return start_dt + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

    # Generate users
    print("[>] Generating users...")
    users = []
    for i in range(1, N_USERS + 1):
        profile = fake.simple_profile()
        users.append({
            "user_id": f"user_{i}",
            "full_name": profile["name"],
            "username": profile["username"],
            "email": profile["mail"],
            "phone": fake.phone_number(),
            "city": fake.city(),
            "created_at": fake.date_time_between(start_date='-2y', end_date='now').isoformat()
        })
    users_df = pd.DataFrame(users)

    # Generate products
    print("[>] Generating products...")
    categories = ["Beverages", "Main", "Dessert", "Snack", "Sauce", "Set Meal"]
    products = []
    for i in range(1, N_PRODUCTS + 1):
        price = round(random.uniform(15000, 350000), -3)
        products.append({
            "product_id": f"prd_{i}",
            "name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "category": random.choice(categories),
            "price": price,
            "created_at": fake.date_time_between(start_date='-3y', end_date='now').isoformat()
        })
    products_df = pd.DataFrame(products)
    products_df["popularity"] = np.random.zipf(a=1.5, size=N_PRODUCTS)

    # Generate orders
    print("[>] Generating orders...")
    orders, order_items = [], []
    product_ids = products_df["product_id"].tolist()
    weights = (products_df["popularity"].astype(float) + 1e-6)
    weights = weights / weights.sum()

    for oid in range(1, N_ORDERS + 1):
        user = users_df.sample(1).iloc[0]
        created_ts = random_timestamp_between(start_dt, end_dt)
        n_items = np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        chosen = np.random.choice(product_ids, size=n_items, replace=False, p=weights)

        total_amount = 0
        for pid in chosen:
            prod = products_df.loc[products_df["product_id"] == pid].iloc[0]
            qty = int(np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1]))
            line_total = qty * prod["price"]
            total_amount += line_total
            order_items.append({
                "order_id": f"ord_{oid}",
                "product_id": pid,
                "quantity": qty,
                "unit_price": prod["price"],
                "line_total": line_total
            })

        status = random.choices(["completed", "cancelled", "returned", "pending"], weights=[0.85, 0.05, 0.03, 0.07])[0]
        orders.append({
            "order_id": f"ord_{oid}",
            "user_id": user["user_id"],
            "order_created_at": created_ts.isoformat(),
            "total_amount": total_amount,
            "payment_method": random.choice(["cash", "card", "momo", "vnpay"]),
            "status": status,
            "branch": fake.city()
        })

    orders_df = pd.DataFrame(orders)
    order_items_df = pd.DataFrame(order_items)

    # Generate sessions
    print("[>] Generating sessions...")
    sessions = []
    for sid in range(1, N_SESSIONS + 1):
        user = users_df.sample(1).iloc[0]
        start_ts = random_timestamp_between(start_dt, end_dt)
        duration = random.randint(30, 60 * 60 * 3)
        end_ts = start_ts + timedelta(seconds=duration)
        sessions.append({
            "session_id": f"sess_{sid}",
            "user_id": user["user_id"],
            "started_at": start_ts.isoformat(),
            "ended_at": end_ts.isoformat(),
            "duration_seconds": duration
        })
    sessions_df = pd.DataFrame(sessions)

    # Inject duplicates
    print("[>] Injecting duplicates...")
    users_df = pd.concat([users_df, users_df.sample(max(1, int(len(users_df)*PCT_DUPLICATES)), replace=True)], ignore_index=True)
    orders_df = pd.concat([orders_df, orders_df.sample(max(1, int(len(orders_df)*PCT_DUPLICATES)), replace=True)], ignore_index=True)

    # Save
    print("[>] Saving files...")
    users_df.to_csv(os.path.join(OUT_DIR, "users.csv"), index=False)
    products_df.to_csv(os.path.join(OUT_DIR, "products.csv"), index=False)
    orders_df.to_csv(os.path.join(OUT_DIR, "orders.csv"), index=False)
    order_items_df.to_csv(os.path.join(OUT_DIR, "order_items.csv"), index=False)
    sessions_df.to_csv(os.path.join(OUT_DIR, "sessions.csv"), index=False)

    print(f"[DONE] Generated {len(users_df)} users, {len(products_df)} products, {len(orders_df)} orders, {len(order_items_df)} order_items, {len(sessions_df)} sessions")


# ========== INGEST FUNCTIONS ==========
@task
def resolve_target_bucket() -> str:
    """Resolve the destination GCS bucket from Airflow Variable or environment."""
    bucket = Variable.get(BUCKET_VARIABLE_KEY, default_var=os.getenv(BUCKET_ENV_VAR))
    if not bucket:
        bucket = GCS_BUCKET  # Fallback to GCS_BUCKET
    return bucket


@task
def discover_files(source_dir: str, object_prefix: str, bucket: str) -> List[Dict[str, str]]:
    """Build upload jobs for each file in the source directory."""
    upload_date = datetime.utcnow().strftime(UPLOAD_DATE_FORMAT)
    base_prefix = object_prefix.strip("/")
    run_prefix = f"{base_prefix}/{upload_date}" if base_prefix else upload_date

    path = Path(source_dir)
    if not path.exists() or not path.is_dir():
        raise AirflowException(f"Source directory '{source_dir}' is not available inside the container.")

    files = sorted(p for p in path.iterdir() if p.is_file() and p.suffix.lower() == ".csv")
    if not files:
        raise AirflowException(f"No CSV files found under '{source_dir}'.")

    jobs: List[Dict[str, str]] = []
    for file_path in files:
        mime_type, _ = mimetypes.guess_type(file_path.name)
        object_name = f"{run_prefix.rstrip('/')}/{file_path.name}" if run_prefix else file_path.name
        jobs.append(
            {
                "bucket": bucket,
                "source": str(file_path),
                "object_name": object_name,
                "mime_type": mime_type or "text/csv",
            }
        )
    return jobs


@task
def upload_to_gcs(job: Dict[str, str], gcp_conn_id: str = GCP_CONN_ID) -> str:
    """Upload a single file described by the job mapping to GCS."""
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    hook.upload(
        bucket_name=job["bucket"],
        object_name=job["object_name"],
        filename=job["source"],
        mime_type=job["mime_type"],
    )
    return job["object_name"]


# ========== LOAD FUNCTIONS ==========
def load_table_to_bq(table_name: str, source_uri: str):
    """Load CSV from GCS to BigQuery retail_raw dataset."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

    # Define explicit schema for users table to avoid autodetect issues
    if table_name == "users":
        schema = [
            bigquery.SchemaField('user_id', 'STRING'),
            bigquery.SchemaField('full_name', 'STRING'),
            bigquery.SchemaField('username', 'STRING'),
            bigquery.SchemaField('email', 'STRING'),
            bigquery.SchemaField('phone', 'STRING'),
            bigquery.SchemaField('city', 'STRING'),
            bigquery.SchemaField('created_at', 'STRING'),
        ]
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
    else:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

    # Load all data to retail_raw table
    print(f"Loading data to {table_id}...")
    load_job = client.load_table_from_uri(
        source_uri,
        table_id,
        location=BQ_LOCATION,
        job_config=job_config,
    )
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows to {table_id}")


def deduplicate_table(table_name: str):
    """Remove duplicate rows from a BigQuery table."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

    print(f"Deduplicating table {table_id}...")

    # Get row count before deduplication
    result = client.query(f"SELECT COUNT(*) as count FROM `{table_id}`", location=BQ_LOCATION).result()
    rows_before = list(result)[0].count

    # Create table with unique rows only
    dedupe_query = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    SELECT DISTINCT * FROM `{table_id}`
    """

    query_job = client.query(dedupe_query, location=BQ_LOCATION)
    query_job.result()

    # Get row count after deduplication
    result_table = client.get_table(table_id)
    rows_after = result_table.num_rows
    duplicates_removed = rows_before - rows_after

    print(f"Deduplication complete: {rows_before} rows -> {rows_after} rows (removed {duplicates_removed} duplicates)")


def dbt_cmd(subcommand: str, extra: str = "") -> str:
    """
    Generate dbt command with standard options.
    Example: dbt_cmd("deps"), dbt_cmd("run", "--select path:models/bronze")
    """
    return (
        f"dbt {subcommand} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"{extra}".strip()
    )


@dag(
    dag_id="retail_pipeline",
    description="Complete retail data pipeline: Ingest CSV to GCS, Load to BigQuery, and run dbt transformations",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "ingest", "load", "dbt", "transform"],
)
def retail_pipeline() -> None:
    # ========== GENERATE SAMPLE DATA STAGE ==========
    gen_data = PythonOperator(
        task_id="generate_sample_data",
        python_callable=generate_sample_data,
    )

    # ========== INGEST STAGE ==========
    bucket = resolve_target_bucket()
    jobs = discover_files(
        source_dir=os.getenv(SOURCE_DIR_ENV_VAR, DEFAULT_SOURCE_DIR),
        object_prefix=os.getenv(OBJECT_PREFIX_ENV_VAR, DEFAULT_OBJECT_PREFIX),
        bucket=bucket,
    )
    ingest_tasks = upload_to_gcs.expand(job=jobs)

    # ========== LOAD STAGE ==========
    load_tasks = []
    dedupe_tasks = []
    for table_name in TABLE_LIST:
        # Pattern to match all date folders (e.g., 2025-10-07, 2025-10-08, etc.)
        source_uri = f"gs://{GCS_BUCKET}/*/{table_name}.csv"

        load_task = PythonOperator(
            task_id=f"load_{table_name}",
            python_callable=load_table_to_bq,
            op_kwargs={"table_name": table_name, "source_uri": source_uri},
        )
        load_tasks.append(load_task)

        # Deduplicate task after loading
        dedupe_task = PythonOperator(
            task_id=f"dedupe_{table_name}",
            python_callable=deduplicate_table,
            op_kwargs={"table_name": table_name},
        )
        dedupe_tasks.append(dedupe_task)

        # Set dependency: load -> dedupe for each table
        load_task >> dedupe_task

    # ========== DBT STAGE ==========
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_cmd("deps"),
    )

    run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=dbt_cmd("run", "--select path:models/bronze"),
    )

    test_bronze = BashOperator(
        task_id="dbt_test_bronze",
        bash_command=dbt_cmd("test", "--select path:models/bronze"),
    )

    run_transform = BashOperator(
        task_id="dbt_run_silver_gold",
        bash_command=dbt_cmd("run", "--select path:models/silver path:models/gold"),
    )

    test_transform = BashOperator(
        task_id="dbt_test_silver_gold",
        bash_command=dbt_cmd("test", "--select path:models/silver path:models/gold"),
    )

    # ========== PIPELINE FLOW ==========
    # Generate sample data -> Ingest CSV to GCS -> Load to BigQuery -> Deduplicate -> Run dbt transformations
    gen_data >> ingest_tasks >> load_tasks
    dedupe_tasks >> dbt_deps >> run_bronze >> test_bronze >> run_transform >> test_transform


dag_instance = retail_pipeline()
