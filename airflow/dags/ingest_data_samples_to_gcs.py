"""Airflow DAG to upload local data_samples CSV files to Google Cloud Storage."""
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

GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
BUCKET_VARIABLE_KEY = "GCS_INGEST_BUCKET"
BUCKET_ENV_VAR = "GCS_TARGET_BUCKET"
SOURCE_DIR_ENV_VAR = "DATA_SAMPLES_DIR"
OBJECT_PREFIX_ENV_VAR = "GCS_OBJECT_PREFIX"
DEFAULT_SOURCE_DIR = "/opt/airflow/data_samples"
DEFAULT_OBJECT_PREFIX = ""
UPLOAD_DATE_FORMAT = "%Y-%m-%d"


@task
def resolve_target_bucket() -> str:
    """Resolve the destination GCS bucket from Airflow Variable or environment."""
    bucket = Variable.get(BUCKET_VARIABLE_KEY, default_var=os.getenv(BUCKET_ENV_VAR))
    if not bucket:
        raise AirflowException(
            "Missing GCS bucket configuration. Set Airflow Variable "
            f"'{BUCKET_VARIABLE_KEY}' or environment variable '{BUCKET_ENV_VAR}'."
        )
    return bucket


@task
def discover_files(source_dir: str, object_prefix: str, bucket: str) -> List[Dict[str, str]]:
    """Build upload jobs for each file in the source directory."""
    ctx = get_current_context()
    upload_date = datetime.utcnow().strftime(UPLOAD_DATE_FORMAT)
    base_prefix = object_prefix.strip("/")
    run_prefix = f"{base_prefix}/{upload_date}" if base_prefix else upload_date

    path = Path(source_dir)
    if not path.exists() or not path.is_dir():
        raise AirflowException(f"Source directory '{source_dir}' is not available inside the container.")

    files = sorted(p for p in path.iterdir() if p.is_file() and p.suffix.lower() == ".csv")
    if not files:
        raise AirflowException(f"No files found under '{source_dir}'.")

    jobs: List[Dict[str, str]] = []
    for file_path in files:
        mime_type, _ = mimetypes.guess_type(file_path.name)
        object_name = f"{run_prefix.rstrip('/')}/{file_path.name}" if run_prefix else file_path.name
        jobs.append(
            {
                "bucket": bucket,
                "source": str(file_path),
                "object_name": object_name,
                "mime_type": mime_type or "application/octet-stream",
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


@dag(
    dag_id="ingest_data_samples_to_gcs",
    description="Uploads local data_samples files to a GCS bucket.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=[ "ingestion"],
)
def ingest_data_samples_to_gcs() -> None:
    bucket = resolve_target_bucket()
    jobs = discover_files(
        source_dir=os.getenv(SOURCE_DIR_ENV_VAR, DEFAULT_SOURCE_DIR),
        object_prefix=os.getenv(OBJECT_PREFIX_ENV_VAR, DEFAULT_OBJECT_PREFIX),
        bucket=bucket,
    )
    upload_to_gcs.expand(job=jobs)


dag = ingest_data_samples_to_gcs()
