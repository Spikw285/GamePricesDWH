#!/bin/bash
set -e

echo "=== Installing dbt ==="
pip install --quiet --no-cache-dir \
    dbt-core \
    dbt-clickhouse

echo "Waiting for postgres..."
sleep 10

airflow db migrate

airflow users create \
  --username "${AIRFLOW_USER}" \
  --password "${AIRFLOW_PASSWORD}" \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com || true

airflow webserver &
airflow scheduler