from __future__ import annotations

import os
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.sdk import dag

class ConfigError(RuntimeError):
    """Raised when required environment variables are missing."""

# --- Cấu hình ---
# Các biến môi trường này vẫn được đọc từ docker-compose.yaml của bạn
PROJECT_ID = os.getenv("GCP_PROJECT")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt/retail")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt/retail/profiles")

if not PROJECT_ID:
    raise ConfigError("GCP_PROJECT must be set for dbt to run.")

# --- Định nghĩa DAG ---
@dag(
    dag_id="dbt_pipeline",
    description="Runs dbt deps, bronze cleanup, and silver/gold transforms in sequence.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "transform"],
)
def dbt_pipeline() -> None:
    # Lệnh dbt cơ bản, có thể được gọi trực tiếp vì đã được cài trong Dockerfile
    dbt_base_command = f"dbt --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

    # Tất cả các task sẽ chạy trong thư mục dự án dbt
    # Không cần truyền biến env nữa vì profiles.yml đã đủ thông tin
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{dbt_base_command} deps",
    )

    run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=f"{dbt_base_command} run --select path:models/bronze",
    )

    test_bronze = BashOperator(
        task_id="dbt_test_bronze",
        bash_command=f"{dbt_base_command} test --select path:models/bronze",
    )

    run_transform = BashOperator(
        task_id="dbt_run_silver_gold",
        bash_command=f"{dbt_base_command} run --select path:models/silver path:models/gold",
    )

    test_transform = BashOperator(
        task_id="dbt_test_silver_gold",
        bash_command=f"{dbt_base_command} test --select path:models/silver path:models/gold",
    )

    dbt_deps >> run_bronze >> test_bronze >> run_transform >> test_transform

dag_instance = dbt_pipeline()