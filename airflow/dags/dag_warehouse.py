"""
dag_warehouse — синхронизация PG → ClickHouse и Data Quality проверки.
Запускается автоматически когда dag_ingestion публикует DS_POSTGRES_READY.
По завершению публикует Dataset → запускает dag_transform.
"""

import os
import logging
from datetime import timedelta, datetime

import clickhouse_connect

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

DS_POSTGRES_READY  = Dataset("postgres://games/prices")    # слушаем этот
DS_WAREHOUSE_READY = Dataset("clickhouse://default/marts") # публикуем этот

DQ_CHECKS = {
    "games":  ["game_id", "title"],
    "prices": ["game_id", "store_id", "price", "currency", "recorded_at"],
    "stores": ["store_id", "name"],
}
DQ_NULL_THRESHOLD_PCT = 1.0

default_args = {
    "owner":      "airflow",
    "retries":     2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dag_warehouse",
    description="PostgreSQL → ClickHouse sync + Data Quality",
    schedule=[DS_POSTGRES_READY],  # триггер — Dataset от dag_ingestion
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["warehouse", "dq"],
)

# ── helpers ───────────────────────────────────────────────────────────────────

def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "default"),
        secure=False,
    )

# ── tasks ─────────────────────────────────────────────────────────────────────

def sync_pg_to_clickhouse(**_):
    import sys
    sys.path.insert(0, "/opt/airflow/ingestion")
    from pg_to_clickhouse import run
    run()


def run_dq_checks(**_):
    ch     = get_ch_client()
    issues = []

    for table, columns in DQ_CHECKS.items():
        total = ch.query(f"SELECT count() FROM {table}").first_row[0]
        if total == 0:
            logging.warning("DQ: таблица %s пустая, пропускаем", table)
            continue

        for col in columns:
            null_count = ch.query(
                f"SELECT countIf({col} IS NULL OR toString({col}) = '') FROM {table}"
            ).first_row[0]
            null_pct = null_count / total * 100
            status   = "OK" if null_pct <= DQ_NULL_THRESHOLD_PCT else "FAIL"
            logging.info("DQ [%s] %s.%s: %.2f%%", status, table, col, null_pct)

            if null_pct > DQ_NULL_THRESHOLD_PCT:
                issues.append(
                    f"{table}.{col}: {null_pct:.2f}% NULL ({null_count}/{total})"
                )

    if issues:
        msg = "DQ FAIL:\n" + "\n".join(f"  • {i}" for i in issues)
        logging.error(msg)
        raise RuntimeError(msg)

    logging.info("DQ: все проверки пройдены ✓")

# ── wiring ────────────────────────────────────────────────────────────────────

t_sync = PythonOperator(task_id="sync_pg_to_ch", python_callable=sync_pg_to_clickhouse, dag=dag)
t_dq   = PythonOperator(
    task_id="data_quality_check",
    python_callable=run_dq_checks,
    outlets=[DS_WAREHOUSE_READY],  # публикуем Dataset после успешного DQ
    dag=dag,
)

t_sync >> t_dq