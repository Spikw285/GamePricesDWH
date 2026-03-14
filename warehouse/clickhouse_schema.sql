CREATE TABLE IF NOT EXISTS developers (
    developer_id UInt32,
    name String
) ENGINE = ReplacingMergeTree()
ORDER BY developer_id;

CREATE TABLE IF NOT EXISTS publishers (
    publisher_id UInt32,
    name String
) ENGINE = ReplacingMergeTree()
ORDER BY publisher_id;

CREATE TABLE IF NOT EXISTS games (
    game_id String,
    title String,
    slug String,
    release_date Nullable(Date),
    developer_id Nullable(UInt32),
    publisher_id Nullable(UInt32)
) ENGINE = ReplacingMergeTree()
ORDER BY game_id;

CREATE TABLE IF NOT EXISTS stores (
    store_id UInt32,
    name String
) ENGINE = ReplacingMergeTree()
ORDER BY store_id;

CREATE TABLE IF NOT EXISTS prices (
    price_id UInt64,
    game_id String,
    store_id UInt32,
    price Decimal(10,2),
    regular_price Decimal(10,2),
    discount Int32,
    currency String,
    recorded_at DateTime
) ENGINE = MergeTree()
ORDER BY (recorded_at, game_id);