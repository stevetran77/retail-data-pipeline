from __future__ import annotations

import os
import re
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.getenv("GCP_PROJECT")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET", "retail_raw")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
TABLE_LIST_STR = os.getenv("BQ_TABLE_LIST")


class ConfigError(RuntimeError):
    """Raised when the DAG configuration is incomplete."""


if not all([PROJECT_ID, GCS_BUCKET, BQ_DATASET, TABLE_LIST_STR]):
    raise ConfigError(
        "Missing required environment variables. "
        "Ensure GCP_PROJECT, GCS_BUCKET, BQ_DATASET, and BQ_TABLE_LIST are set."
    )

TABLE_LIST = [table.strip() for table in TABLE_LIST_STR.split(",") if table.strip()]


def _normalise_task_id(table_name: str) -> str:
    """Convert a BigQuery table name to a valid Airflow task id."""
    safe = re.sub(r"[^0-9A-Za-z_]", "_", table_name)
    return f"bq_load_{safe.lower() or 'table'}"


@dag(
    dag_id="load_gcs_to_bq",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["load"],
)
def load_gcs_to_bq_dynamically() -> None:
    for table_name in TABLE_LIST:
        task_prefix = _normalise_task_id(table_name)
        staging_table_name = f"{table_name}__staging"

        # Stage the new slice for the day before merging it into the canonical table.
        load_to_staging = BigQueryInsertJobOperator(
            task_id=f"{task_prefix}_load_stage",
            location=BQ_LOCATION,
            configuration={
                "load": {
                    "sourceUris": [f"gs://{GCS_BUCKET}/{{{{ ds }}}}/{table_name}.csv"],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": BQ_DATASET,
                        "tableId": staging_table_name,
                    },
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "autodetect": True,
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
        )

        deduplicate_target = BigQueryInsertJobOperator(
            task_id=f"{task_prefix}_dedupe",
            location=BQ_LOCATION,
            configuration={
                "query": {
                    "query": f"""
DECLARE target_exists BOOL DEFAULT EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{BQ_DATASET}.__TABLES_SUMMARY__`
  WHERE table_id = '{table_name}'
);

IF target_exists THEN
  CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_DATASET}.{table_name}` AS
  SELECT * FROM (
    SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.{table_name}`
    UNION DISTINCT
    SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.{staging_table_name}`
  );
ELSE
  CREATE TABLE `{PROJECT_ID}.{BQ_DATASET}.{table_name}` AS
  SELECT DISTINCT * FROM `{PROJECT_ID}.{BQ_DATASET}.{staging_table_name}`;
END IF;

DROP TABLE IF EXISTS `{PROJECT_ID}.{BQ_DATASET}.{staging_table_name}`;
""",
                    "useLegacySql": False,
                }
            },
        )

        load_to_staging >> deduplicate_target


dag = load_gcs_to_bq_dynamically()
