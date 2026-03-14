{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(game_id)',
        description  = 'Best current deal per game (highest discount across all stores)'
    )
}}

/*
  From the latest price snapshot we pick the store with the highest
  discount for each game. Only games with at least some discount are included.
*/
WITH best AS (
    SELECT
        game_id,
        game_title,
        developer_name,
        publisher_name,
        release_date,
        store_name,
        price,
        regular_price,
        discount,
        currency,
        recorded_at,
        row_number() OVER (
            PARTITION BY game_id
            ORDER BY discount DESC, price ASC
        ) AS rn
    FROM {{ ref('dm_prices_latest') }}
    WHERE discount > 0
)

SELECT
    game_id,
    game_title,
    developer_name,
    publisher_name,
    release_date,
    store_name       AS best_store,
    price            AS best_price,
    regular_price,
    discount         AS best_discount,
    currency,
    recorded_at
FROM best
WHERE rn = 1
ORDER BY discount DESC