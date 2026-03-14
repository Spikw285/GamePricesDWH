{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(game_id, store_id, recorded_at)',
        partition_by = "toYYYYMM(recorded_at)",
        custom_settings = {'enable_analyzer': 0},
        description  = 'Full price history per (game, store) — used for trend charts in BI'
    )
}}

WITH prices_stores AS (
    SELECT
        p.game_id,
        s.store_id AS store_id,
        s.store_name AS store_name,
        p.price,
        p.regular_price,
        p.discount,
        p.currency,
        p.recorded_at
    FROM {{ ref('stg_prices') }} AS p
    JOIN {{ ref('stg_stores') }} AS s ON s.store_id = p.store_id
)

SELECT
    ps.game_id,
    g.title AS game_title,
    ps.store_id,
    ps.store_name,
    ps.price,
    ps.regular_price,
    ps.discount,
    ps.currency,
    ps.recorded_at,
    toDate(ps.recorded_at) AS snapshot_date
FROM prices_stores AS ps
JOIN {{ ref('stg_games') }} AS g ON g.game_id = ps.game_id