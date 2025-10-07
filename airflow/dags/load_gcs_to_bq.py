from __future__ import annotations

import os
import re
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- ĐỌC CÁC BIẾN MÔI TRƯỜNG ---
PROJECT_ID = os.getenv("GCP_PROJECT")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET", "retail_raw")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
# Biến mới chứa danh sách các bảng, ví dụ: 'users,products,orders'
TABLE_LIST_STR = os.getenv("BQ_TABLE_LIST")


class ConfigError(RuntimeError):
    """Raised when the DAG configuration is incomplete."""


# --- KIỂM TRA CẤU HÌNH ---
if not all([PROJECT_ID, GCS_BUCKET, BQ_DATASET, TABLE_LIST_STR]):
    raise ConfigError(
        "Missing required environment variables. "
        "Ensure GCP_PROJECT, GCS_BUCKET, BQ_DATASET, and BQ_TABLE_LIST are set."
    )

# Chuyển chuỗi thành một danh sách các tên bảng
TABLE_LIST = [table.strip() for table in TABLE_LIST_STR.split(",")]


def _normalise_task_id(table_name: str) -> str:
    """Convert a BigQuery table name to a valid Airflow task id."""
    safe = re.sub(r"[^0-9A-Za-z_]", "_", table_name.strip() or "table")
    return f"bq_load_{safe.lower()}"


# --- ĐỊNH NGHĨA DAG ---
@dag(
    dag_id="load_gcs_to_bq_", # Đổi tên dag_id để tránh nhầm lẫn
    start_date=datetime(2024, 1, 1),
    schedule=None, 
    catchup=False,
    tags=["load"],
)
def load_gcs_to_bq_dynamically():
    for table_name in TABLE_LIST:
        BigQueryInsertJobOperator(
            task_id=_normalise_task_id(table_name),
            location=BQ_LOCATION,
            configuration={
                "load": {
                    # ĐÂY LÀ PHẦN QUAN TRỌNG NHẤT:
                    # {{ ds }} sẽ tự động được thay thế bằng ngày thực thi
                    # Ví dụ: 'gs://retail-data-pipeline-raw/2025-10-08/users.csv'
                    "sourceUris": [
                        f"gs://{GCS_BUCKET}/{{{{ ds }}}}/{table_name}.csv"
                    ],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": BQ_DATASET,
                        "tableId": table_name,
                    },
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "autodetect": True,
                    "writeDisposition": "WRITE_APPEND", # Hoặc "WRITE_TRUNCATE" nếu muốn ghi đè
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
        )

# Khởi tạo DAG
the_dag = load_gcs_to_bq_dynamically()