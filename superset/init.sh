#!/bin/bash
# Superset initialization script.
# Runs once on container start: installs the ClickHouse driver,
# initialises the DB, creates the admin user, then starts the server.
set -e

echo "=== Installing ClickHouse driver for Superset ==="
pip install --quiet --no-cache-dir clickhouse-connect

echo "=== Superset DB upgrade ==="
superset db upgrade

echo "=== Creating admin user ==="
superset fab create-admin \
    --username  "${ADMIN_USERNAME}" \
    --password  "${ADMIN_PASSWORD}" \
    --email     "${ADMIN_EMAIL}" \
    --firstname "${ADMIN_FIRST_NAME}" \
    --lastname  "${ADMIN_LAST_NAME}" || true

echo "=== Superset init ==="
superset init

echo "=== Starting Superset ==="
gunicorn \
    --bind "0.0.0.0:8088" \
    --workers 2 \
    --timeout 120 \
    --access-logfile "-" \
    --error-logfile "-" \
    "superset.app:create_app()"