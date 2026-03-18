"""
dag_transform — запуск dbt (run → test → docs).
Запускается автоматически когда dag_warehouse публикует DS_WAREHOUSE_READY.
"""

import os
import logging
import subprocess
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

DS_WAREHOUSE_READY = Dataset("clickhouse://default/marts")  # слушаем этот

DBT_PROJECT_DIR  = os.getenv("DBT_PROJECT_DIR",  "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET       = os.getenv("DBT_TARGET", "dev")

default_args = {
    "owner":      "airflow",
    "retries":     1,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    dag_id="dag_transform",
    description="dbt run → test → docs",
    schedule=[DS_WAREHOUSE_READY],  # триггер — Dataset от dag_warehouse
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "transform"],
)

# ── helpers ───────────────────────────────────────────────────────────────────

def _dbt(command: list[str]) -> None:
    full_cmd = [
        "dbt", *command,
        "--project-dir",  DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--target",       DBT_TARGET,
    ]
    logging.info("Запуск: %s", " ".join(full_cmd))
    result = subprocess.run(full_cmd, capture_output=True, text=True)
    logging.info(result.stdout)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise RuntimeError(f"dbt {command[0]} завершился с ошибкой (exit {result.returncode})")

# ── tasks ─────────────────────────────────────────────────────────────────────

def dbt_run(**_):      _dbt(["run"])
def dbt_test(**_):     _dbt(["test"])
def dbt_docs(**_):     _dbt(["docs", "generate"])

# ── wiring ────────────────────────────────────────────────────────────────────

with TaskGroup(group_id="dbt", dag=dag) as tg_dbt:
    t_run  = PythonOperator(task_id="run",           python_callable=dbt_run,  dag=dag)
    t_test = PythonOperator(task_id="test",          python_callable=dbt_test, dag=dag)
    t_docs = PythonOperator(task_id="docs_generate", python_callable=dbt_docs, dag=dag)
    t_run >> t_test >> t_docs