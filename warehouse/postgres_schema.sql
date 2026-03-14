CREATE TABLE IF NOT EXISTS developers (
    developer_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publishers (
    publisher_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS games (
    game_id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    slug TEXT,
    release_date DATE,
    developer_id INT REFERENCES developers(developer_id),
    publisher_id INT REFERENCES publishers(publisher_id)
);

CREATE TABLE IF NOT EXISTS stores (
    store_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
    price_id BIGSERIAL PRIMARY KEY,
    game_id BIGINT REFERENCES games(game_id),
    store_id INT REFERENCES stores(store_id),
    price NUMERIC(10,2),
    regular_price NUMERIC(10,2),
    discount INT,
    currency TEXT,
    recorded_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prices_game ON prices(game_id);
CREATE INDEX IF NOT EXISTS idx_prices_time ON prices(recorded_at);