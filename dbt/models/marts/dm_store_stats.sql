{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = 'store_name',
        description  = 'Aggregated store-level statistics based on latest prices'
    )
}}

SELECT
    store_id,
    store_name,
    count(DISTINCT game_id)                     AS unique_games,
    round(avg(price), 2)                        AS avg_current_price,
    round(avg(regular_price), 2)                AS avg_regular_price,
    round(avg(discount), 2)                     AS avg_discount_pct,
    min(price)                                  AS min_price,
    max(price)                                  AS max_price,
    countIf(discount > 0)                       AS games_on_sale,
    round(countIf(discount > 0) / count() * 100, 1) AS pct_on_sale,
    max(recorded_at)                            AS last_snapshot_at
FROM {{ ref('dm_prices_latest') }}
GROUP BY store_id, store_name