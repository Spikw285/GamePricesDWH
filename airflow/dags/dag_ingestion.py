"""
dag_ingestion — сбор данных с ITAD API и загрузка в PostgreSQL.
Не зависит ни от каких других DAG'ов.
По завершению публикует Dataset → запускает dag_warehouse.
"""

import os
import time
import logging
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

# ── Dataset, который этот DAG публикует после успешного завершения ──
DS_POSTGRES_READY = Dataset("postgres://games/prices")

BASE_URL = "https://api.isthereanydeal.com"
DELAY    = 0.5

default_args = {
    "owner":      "airflow",
    "retries":     2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dag_ingestion",
    description="ITAD API → PostgreSQL",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["itad", "ingestion"],
)

# ── helpers ──────────────────────────────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB", "games"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

# ── tasks ─────────────────────────────────────────────────────────────────────

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

    for info in games_info:
        for table, key in [("developers", "developers"), ("publishers", "publishers")]:
            if info.get(key):
                name = info[key][0]["name"]
                cur.execute(
                    f"INSERT INTO {table} (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                    (name,),
                )

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

    for game in prices_data:
        for deal in game.get("deals", []):
            cur.execute("INSERT INTO stores (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                        (deal["shop"]["name"],))

    cur.execute("SELECT store_id, name FROM stores")
    store_map = {name: sid for sid, name in cur.fetchall()}

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

# ── wiring ────────────────────────────────────────────────────────────────────

t_ids   = PythonOperator(task_id="fetch_popular_ids", python_callable=fetch_popular_ids, dag=dag)
t_info  = PythonOperator(task_id="fetch_games_info",  python_callable=fetch_games_info,  dag=dag)
t_price = PythonOperator(task_id="fetch_prices",      python_callable=fetch_prices,      dag=dag)
t_pg    = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    outlets=[DS_POSTGRES_READY],  # публикуем Dataset после успешной загрузки
    dag=dag,
)

t_ids >> [t_info, t_price] >> t_pg