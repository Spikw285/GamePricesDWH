"""
itad_ingest_pipeline — полный DAG:
  1. Забираем popular game IDs с ITAD API
  2. Загружаем game info и prices параллельно
  3. Пишем в PostgreSQL
  4. Синхронизируем PG → ClickHouse (incremental)
  5. Data Quality проверка raw-слоя (порог: >1% NULL → fail)
  6. dbt run   (staging + marts)
  7. dbt test  (schema + data tests)
  8. dbt docs generate
"""

import os
import time
import logging
import subprocess
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
import clickhouse_connect
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ─────────────────────────── constants ────────────────────────────────────────

BASE_URL = "https://api.isthereanydeal.com"
DELAY    = 0.5

DBT_PROJECT_DIR  = os.getenv("DBT_PROJECT_DIR",  "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET       = os.getenv("DBT_TARGET", "dev")

# Fields to DQ-check: {table: [column, ...]}
DQ_CHECKS = {
    "games":  ["game_id", "title"],
    "prices": ["game_id", "store_id", "price", "currency", "recorded_at"],
    "stores": ["store_id", "name"],
}
DQ_NULL_THRESHOLD_PCT = 1.0   # fail DAG if NULL% > this value

# ──────────────────────────── DAG definition ──────────────────────────────────

default_args = {
    "owner":        "airflow",
    "retries":       2,
    "retry_delay":   timedelta(minutes=5),
}

dag = DAG(
    dag_id="itad_ingest_pipeline",
    description="Daily ITAD ingest → PostgreSQL → ClickHouse → DQ → dbt",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["itad", "ingestion", "dbt"],
)

# ──────────────────────────── helpers ─────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB", "games"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "default"),
        secure=False,
    )

# ──────────────────────────── stage 1-4: ingest ───────────────────────────────

def fetch_popular_ids(**context):
    key  = os.getenv("ITAD_API_KEY")
    resp = requests.get(f"{BASE_URL}/stats/most-popular/v1",
                        params={"key": key, "limit": 100})
    resp.raise_for_status()
    ids = [game["id"] for game in resp.json()]
    logging.info("Получено %d игр", len(ids))
    context["ti"].xcom_push(key="game_ids", value=ids)


def fetch_games_info(**context):
    key      = os.getenv("ITAD_API_KEY")
    game_ids = context["ti"].xcom_pull(key="game_ids", task_ids="fetch_popular_ids")
    games_info = []
    for i, gid in enumerate(game_ids):
        resp = requests.get(f"{BASE_URL}/games/info/v2",
                            params={"key": key, "id": gid})
        if resp.status_code == 404:
            logging.warning("Игра %s не найдена, пропускаем", gid)
            continue
        resp.raise_for_status()
        games_info.append(resp.json())
        if (i + 1) % 10 == 0:
            logging.info("games/info: %d/%d", i + 1, len(game_ids))
        time.sleep(DELAY)
    context["ti"].xcom_push(key="games_info", value=games_info)
    logging.info("Загружено info для %d игр", len(games_info))


def fetch_prices(**context):
    key      = os.getenv("ITAD_API_KEY")
    game_ids = context["ti"].xcom_pull(key="game_ids", task_ids="fetch_popular_ids")
    all_prices = []
    for i in range(0, len(game_ids), 200):
        batch = game_ids[i:i + 200]
        resp  = requests.post(
            f"{BASE_URL}/games/prices/v3",
            params={"key": key, "country": "US"},
            json=batch,
        )
        resp.raise_for_status()
        all_prices.extend(resp.json())
        time.sleep(DELAY)
    context["ti"].xcom_push(key="prices_data", value=all_prices)
    logging.info("Загружено цен для %d игр", len(all_prices))


def load_to_postgres(**context):
    games_info  = context["ti"].xcom_pull(key="games_info",  task_ids="fetch_games_info")
    prices_data = context["ti"].xcom_pull(key="prices_data", task_ids="fetch_prices")

    conn = get_pg_conn()
    cur  = conn.cursor()

    # Developers / publishers
    for info in games_info:
        for table, key in [("developers", "developers"), ("publishers", "publishers")]:
            if info.get(key):
                name = info[key][0]["name"]
                cur.execute(
                    f"INSERT INTO {table} (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                    (name,),
                )

    # Games
    for info in games_info:
        dev_id = pub_id = None
        if info.get("developers"):
            cur.execute("SELECT developer_id FROM developers WHERE name = %s",
                        (info["developers"][0]["name"],))
            row = cur.fetchone()
            if row:
                dev_id = row[0]
        if info.get("publishers"):
            cur.execute("SELECT publisher_id FROM publishers WHERE name = %s",
                        (info["publishers"][0]["name"],))
            row = cur.fetchone()
            if row:
                pub_id = row[0]
        cur.execute("""
            INSERT INTO games (game_id, title, slug, release_date, developer_id, publisher_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE SET
                title        = EXCLUDED.title,
                slug         = EXCLUDED.slug,
                release_date = EXCLUDED.release_date,
                developer_id = EXCLUDED.developer_id,
                publisher_id = EXCLUDED.publisher_id
        """, (info["id"], info["title"], info.get("slug"), info.get("releaseDate"),
               dev_id, pub_id))

    # Stores
    for game in prices_data:
        for deal in game.get("deals", []):
            cur.execute("INSERT INTO stores (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                        (deal["shop"]["name"],))

    cur.execute("SELECT store_id, name FROM stores")
    store_map = {name: sid for sid, name in cur.fetchall()}

    # Prices
    rows        = []
    recorded_at = datetime.now(timezone.utc)
    for game in prices_data:
        for deal in game.get("deals", []):
            store_id = store_map.get(deal["shop"]["name"])
            if store_id is None:
                continue
            rows.append((
                game["id"], store_id,
                deal["price"]["amount"], deal["regular"]["amount"],
                deal["cut"], deal["price"]["currency"], recorded_at,
            ))

    if rows:
        execute_values(cur, """
            INSERT INTO prices
                (game_id, store_id, price, regular_price, discount, currency, recorded_at)
            VALUES %s
        """, rows)

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Загружено %d записей цен, %d игр", len(rows), len(games_info))


def sync_pg_to_clickhouse(**_):
    """Incremental sync PG → ClickHouse (reuses pg_to_clickhouse logic)."""
    import sys
    sys.path.insert(0, "/opt/airflow/ingestion")
    from pg_to_clickhouse import run   # noqa: PLC0415
    run()

# ──────────────────────────── stage 5: Data Quality ──────────────────────────

def run_dq_checks(**_):
    """
    Check NULL percentage in critical columns of ClickHouse raw tables.
    Raises RuntimeError (→ DAG fail) if any column exceeds DQ_NULL_THRESHOLD_PCT.
    """
    ch      = get_ch_client()
    issues  = []

    for table, columns in DQ_CHECKS.items():
        # Total row count
        total_result = ch.query(f"SELECT count() FROM {table}")
        total        = total_result.first_row[0]
        if total == 0:
            logging.warning("DQ: таблица %s пустая, пропускаем", table)
            continue

        for col in columns:
            null_result = ch.query(
                f"SELECT countIf({col} IS NULL OR toString({col}) = '') FROM {table}"
            )
            null_count  = null_result.first_row[0]
            null_pct    = null_count / total * 100

            status = "OK" if null_pct <= DQ_NULL_THRESHOLD_PCT else "FAIL"
            logging.info(
                "DQ [%s] %s.%s: %d NULL / %d total = %.2f%% → %s",
                status, table, col, null_count, total, null_pct, status,
            )

            if null_pct > DQ_NULL_THRESHOLD_PCT:
                issues.append(
                    f"{table}.{col}: {null_pct:.2f}% NULL "
                    f"({null_count}/{total}) > threshold {DQ_NULL_THRESHOLD_PCT}%"
                )

    if issues:
        msg = "DQ FAIL — превышен порог NULL:\n" + "\n".join(f"  • {i}" for i in issues)
        logging.error(msg)
        raise RuntimeError(msg)

    logging.info("DQ: все проверки пройдены ✓")

# ──────────────────────────── stage 6: dbt ───────────────────────────────────

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


def dbt_run(**_):
    _dbt(["run"])


def dbt_test(**_):
    _dbt(["test"])


def dbt_docs_generate(**_):
    _dbt(["docs", "generate"])

# ──────────────────────────── task wiring ─────────────────────────────────────

# Stage 1–4: ingest
t_ids   = PythonOperator(task_id="fetch_popular_ids", python_callable=fetch_popular_ids, dag=dag)
t_info  = PythonOperator(task_id="fetch_games_info",  python_callable=fetch_games_info,  dag=dag)
t_price = PythonOperator(task_id="fetch_prices",      python_callable=fetch_prices,      dag=dag)
t_pg    = PythonOperator(task_id="load_to_postgres",  python_callable=load_to_postgres,  dag=dag)
t_sync  = PythonOperator(task_id="sync_pg_to_ch",     python_callable=sync_pg_to_clickhouse, dag=dag)

# Stage 5: DQ
t_dq = PythonOperator(task_id="data_quality_check", python_callable=run_dq_checks, dag=dag)

# Stage 6: dbt as a TaskGroup
with TaskGroup(group_id="dbt", dag=dag) as tg_dbt:
    t_dbt_run  = PythonOperator(task_id="run",           python_callable=dbt_run,          dag=dag)
    t_dbt_test = PythonOperator(task_id="test",          python_callable=dbt_test,          dag=dag)
    t_dbt_docs = PythonOperator(task_id="docs_generate", python_callable=dbt_docs_generate, dag=dag)
    t_dbt_run >> t_dbt_test >> t_dbt_docs

# DAG flow:
#   fetch IDs → [fetch info, fetch prices] (parallel) → load PG → sync CH → DQ → dbt
t_ids >> [t_info, t_price] >> t_pg >> t_sync >> t_dq >> tg_dbt