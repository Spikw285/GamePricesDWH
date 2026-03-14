"""
dbt integration tasks for the Airflow DAG.

Usage — append these tasks to the existing ingest_pipeline.py DAG:

    from airflow.utils.task_group import TaskGroup
    from dbt_runner import make_dbt_task_group

    with dag:
        ...
        t4 >> make_dbt_task_group(dag)

Requirements (add to airflow/requirements.txt):
    dbt-core
    dbt-clickhouse
"""

import os
import subprocess
import logging

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

log = logging.getLogger(__name__)


def _run_dbt(command: list[str]) -> None:
    """Run a dbt CLI command and raise on non-zero exit."""
    full_cmd = [
        "dbt", *command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--target", DBT_TARGET,
    ]
    log.info("Running: %s", " ".join(full_cmd))
    result = subprocess.run(full_cmd, capture_output=True, text=True)
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt command failed (exit {result.returncode})")


def dbt_run(**_):
    """Materialise all dbt models (staging → marts)."""
    _run_dbt(["run"])


def dbt_test(**_):
    """Run dbt schema + data tests. Fails DAG if any critical test fails."""
    _run_dbt(["test"])


def dbt_docs_generate(**_):
    """Regenerate dbt docs (catalog.json + manifest.json)."""
    _run_dbt(["docs", "generate"])


def make_dbt_task_group(dag):
    """
    Returns a TaskGroup with run → test → docs tasks.
    Attach it after the raw-load task in your DAG.

    Example:
        load_task >> make_dbt_task_group(dag)
    """
    with TaskGroup(group_id="dbt", dag=dag) as tg:
        t_run = PythonOperator(
            task_id="dbt_run",
            python_callable=dbt_run,
            dag=dag,
        )
        t_test = PythonOperator(
            task_id="dbt_test",
            python_callable=dbt_test,
            dag=dag,
        )
        t_docs = PythonOperator(
            task_id="dbt_docs_generate",
            python_callable=dbt_docs_generate,
            dag=dag,
        )
        t_run >> t_test >> t_docs

    return tg