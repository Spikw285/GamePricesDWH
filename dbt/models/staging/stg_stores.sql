{{
    config(
        materialized = 'table',
        description  = 'Cleaned stores dimension'
    )
}}

SELECT
    store_id,
    trim(name) AS store_name
FROM {{ source('raw', 'stores') }}