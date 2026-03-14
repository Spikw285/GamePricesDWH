import os
import logging
import psycopg2
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PG_CONN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB", "games"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

CH_CLIENT = dict(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123)),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    database=os.getenv("CLICKHOUSE_DB", "default"),
)


def get_pg():
    return psycopg2.connect(**PG_CONN)


def get_ch():
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
    )

def init_clickhouse_schema(ch):
    """Создать таблицы в ClickHouse если не существуют"""
    schema_path = os.path.join(os.path.dirname(__file__), "../warehouse/clickhouse_schema.sql")
    with open(schema_path) as f:
        sql = f.read()
    for statement in sql.strip().split(";"):
        statement = statement.strip()
        if statement:
            ch.command(statement)
    log.info("Схема ClickHouse готова")


def sync_table(pg_cur, ch, table: str, columns: list[str], order_col: str = None):
    """Скопировать таблицу из PG в CH"""
    cols = ", ".join(columns)
    pg_cur.execute(f"SELECT {cols} FROM {table}")
    rows = pg_cur.fetchall()

    if not rows:
        log.info(f"{table}: нет данных")
        return

    ch.insert(table, rows, column_names=columns)
    log.info(f"{table}: перенесено {len(rows)} строк")


def sync_prices_incremental(pg_cur, ch):
    """
    Для prices — только новые записи которых ещё нет в ClickHouse.
    Смотрим на MAX(recorded_at) в CH и берём из PG только то что новее.
    """
    result = ch.query("SELECT max(recorded_at) FROM prices")
    max_recorded_at = result.first_row[0]

    if max_recorded_at and str(max_recorded_at) != "1970-01-01 00:00:00":
        pg_cur.execute("""
            SELECT price_id, game_id, store_id, price, regular_price,
                   discount, currency, recorded_at
            FROM prices
            WHERE recorded_at > %s
        """, (max_recorded_at,))
        log.info(f"prices: incremental load, после {max_recorded_at}")
    else:
        pg_cur.execute("""
            SELECT price_id, game_id, store_id, price, regular_price,
                   discount, currency, recorded_at
            FROM prices
        """)
        log.info("prices: full load")

    rows = pg_cur.fetchall()
    if rows:
        columns = ["price_id", "game_id", "store_id", "price",
                   "regular_price", "discount", "currency", "recorded_at"]
        ch.insert("prices", rows, column_names=columns)
        log.info(f"prices: перенесено {len(rows)} строк")
    else:
        log.info("prices: новых данных нет")


def run():
    log.info("=== Запуск PG → ClickHouse sync ===")

    pg = get_pg()
    pg_cur = pg.cursor()
    ch = get_ch()

    init_clickhouse_schema(ch)

    sync_table(pg_cur, ch, "developers",
               ["developer_id", "name"])

    sync_table(pg_cur, ch, "publishers",
               ["publisher_id", "name"])

    sync_table(pg_cur, ch, "games",
               ["game_id", "title", "slug", "release_date",
                "developer_id", "publisher_id"])

    sync_table(pg_cur, ch, "stores",
               ["store_id", "name"])

    sync_prices_incremental(pg_cur, ch)

    pg_cur.close()
    pg.close()
    log.info("=== Sync завершён ===")


if __name__ == "__main__":
    run()