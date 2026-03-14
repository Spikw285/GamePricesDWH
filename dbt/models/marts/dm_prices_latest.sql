{{
    config(
        materialized = 'table',
        engine       = 'ReplacingMergeTree()',
        order_by     = '(game_id, store_id)',
        custom_settings = {'enable_analyzer': 0},
        description  = 'Latest price snapshot per (game, store) pair'
    )
}}

WITH ranked AS (
    SELECT
        ps.price_id,
        ps.game_id,
        g.title          AS game_title,
        g.developer_name,
        g.publisher_name,
        g.release_date,
        ps.store_id,
        ps.store_name,
        ps.price,
        ps.regular_price,
        ps.discount,
        ps.currency,
        ps.recorded_at,
        row_number() OVER (
            PARTITION BY ps.game_id, ps.store_id
            ORDER BY ps.recorded_at DESC
        ) AS rn
    FROM (
        SELECT
            p.price_id,
            p.game_id,
            p.store_id,
            s.store_name,
            p.price,
            p.regular_price,
            p.discount,
            p.currency,
            p.recorded_at
        FROM {{ ref('stg_prices') }} AS p
        JOIN {{ ref('stg_stores') }} AS s ON s.store_id = p.store_id
    ) AS ps
    JOIN {{ ref('stg_games') }} AS g ON g.game_id = ps.game_id
)

SELECT
    price_id,
    game_id,
    game_title,
    developer_name,
    publisher_name,
    release_date,
    store_id,
    store_name,
    price,
    regular_price,
    discount,
    currency,
    recorded_at
FROM ranked
WHERE rn = 1