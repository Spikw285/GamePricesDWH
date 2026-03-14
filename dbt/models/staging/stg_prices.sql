{{
    config(
        materialized = 'table',
        description  = 'Cleaned and validated price snapshots'
    )
}}

SELECT
    price_id,
    game_id,
    store_id,
    greatest(price,         0) AS price,
    greatest(regular_price, 0) AS regular_price,
    greatest(least(discount, 100), 0) AS discount,
    upper(trim(currency))      AS currency,
    recorded_at
FROM {{ source('raw', 'prices') }}
WHERE price_id IS NOT NULL
  AND game_id  IS NOT NULL
  AND store_id IS NOT NULL