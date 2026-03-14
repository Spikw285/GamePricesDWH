{{
    config(
        materialized = 'table',
        description  = 'Cleaned games enriched with developer and publisher names'
    )
}}

SELECT
    g.game_id,
    trim(g.title)                              AS title,
    trim(g.slug)                               AS slug,
    g.release_date,
    d.developer_id,
    coalesce(trim(d.name), 'Unknown')          AS developer_name,
    p.publisher_id,
    coalesce(trim(p.name), 'Unknown')          AS publisher_name
FROM {{ source('raw', 'games') }}       AS g
LEFT JOIN {{ source('raw', 'developers') }} AS d ON g.developer_id = d.developer_id
LEFT JOIN {{ source('raw', 'publishers') }} AS p ON g.publisher_id = p.publisher_id