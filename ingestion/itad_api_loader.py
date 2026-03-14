import os
import time
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ITAD_KEY = os.getenv("ITAD_API_KEY")
PG_CONN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB", "games"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
BASE_URL = "https://api.isthereanydeal.com"
DELAY = 0.5  # секунд между запросами


def get_popular_game_ids(limit: int = 100) -> list[str]:
    """GET /stats/most-popular/v1 — возвращает [{position, id, slug, title, ...}]"""
    resp = requests.get(
        f"{BASE_URL}/stats/most-popular/v1",
        params={"key": ITAD_KEY, "limit": limit}
    )
    resp.raise_for_status()
    data = resp.json()
    ids = [game["id"] for game in data]
    log.info(f"Получено {len(ids)} игр из most-popular")
    return ids


def get_game_info(game_id: str) -> dict | None:
    """GET /games/info/v2?id={uuid} — один запрос на одну игру"""
    resp = requests.get(
        f"{BASE_URL}/games/info/v2",
        params={"key": ITAD_KEY, "id": game_id}
    )
    if resp.status_code == 404:
        log.warning(f"Игра {game_id} не найдена")
        return None
    resp.raise_for_status()
    return resp.json()


def get_games_prices(game_ids: list[str]) -> list[dict]:
    """POST /games/prices/v3 — батчами по 200 (это поддерживает массив)"""
    url = f"{BASE_URL}/games/prices/v3"
    all_prices = []

    for i in range(0, len(game_ids), 200):
        batch = game_ids[i:i + 200]
        resp = requests.post(
            url,
            params={"key": ITAD_KEY, "country": "US"},
            json=batch
        )
        resp.raise_for_status()
        all_prices.extend(resp.json())
        log.info(f"games/prices: получено {len(all_prices)}/{len(game_ids)}")
        time.sleep(DELAY)

    return all_prices


def upsert_developer(cur, name: str) -> int:
    cur.execute("""
        INSERT INTO developers (name) VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING developer_id
    """, (name,))
    return cur.fetchone()[0]


def upsert_publisher(cur, name: str) -> int:
    cur.execute("""
        INSERT INTO publishers (name) VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING publisher_id
    """, (name,))
    return cur.fetchone()[0]


def upsert_game(cur, info: dict):
    dev_id = upsert_developer(cur, info["developers"][0]["name"]) if info.get("developers") else None
    pub_id = upsert_publisher(cur, info["publishers"][0]["name"]) if info.get("publishers") else None

    cur.execute("""
        INSERT INTO games (game_id, title, slug, release_date, developer_id, publisher_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            release_date = EXCLUDED.release_date,
            developer_id = EXCLUDED.developer_id,
            publisher_id = EXCLUDED.publisher_id
    """, (
        info["id"],
        info["title"],
        info.get("slug"),
        info.get("releaseDate"),
        dev_id,
        pub_id,
    ))


def upsert_stores(cur, prices_data: list[dict]) -> dict[int, int]:
    """Вставить магазины по числовому ITAD id, вернуть маппинг itad_shop_id -> store_id"""
    shops = {}
    for game in prices_data:
        for deal in game.get("deals", []):
            shop = deal["shop"]
            shops[shop["id"]] = shop["name"]

    for itad_id, name in shops.items():
        cur.execute("""
            INSERT INTO stores (name) VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, (name,))

    cur.execute("SELECT store_id, name FROM stores")
    name_to_id = {name: sid for sid, name in cur.fetchall()}
    return {itad_id: name_to_id[name] for itad_id, name in shops.items()}


def insert_prices(cur, prices_data: list[dict], store_map: dict[int, int]):
    rows = []
    for game in prices_data:
        for deal in game.get("deals", []):
            store_id = store_map.get(deal["shop"]["id"])
            if store_id is None:
                continue
            rows.append((
                game["id"],
                store_id,
                deal["price"]["amount"],
                deal["regular"]["amount"],
                deal["cut"],
                deal["price"]["currency"],
            ))

    if rows:
        execute_values(cur, """
            INSERT INTO prices (game_id, store_id, price, regular_price, discount, currency, recorded_at)
            VALUES %s
        """, [(r[0], r[1], r[2], r[3], r[4], r[5], "NOW()") for r in rows])
        log.info(f"Вставлено {len(rows)} записей цен")


def run(limit: int = 100):
    log.info("=== Запуск ITAD ingestion ===")

    game_ids = get_popular_game_ids(limit)

    # games/info — один запрос на игру, собираем всё что получилось
    games_info = []
    for i, gid in enumerate(game_ids):
        info = get_game_info(gid)
        if info:
            games_info.append(info)
        if (i + 1) % 10 == 0:
            log.info(f"games/info: {i + 1}/{len(game_ids)}")
        time.sleep(DELAY)

    # prices — батч
    prices_data = get_games_prices(game_ids)

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            for info in games_info:
                upsert_game(cur, info)
            log.info(f"Upsert {len(games_info)} игр")

            store_map = upsert_stores(cur, prices_data)
            insert_prices(cur, prices_data, store_map)
        conn.commit()

    log.info("=== Ingestion завершён ===")


if __name__ == "__main__":
    run(limit=100)