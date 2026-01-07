
{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT
        symbol,
        window_start,
        window_end,
        ROUND(CAST(avg_price AS DOUBLE), 2)    as price,
        ROUND(CAST(total_volume AS DOUBLE), 3) as quantity
    FROM {{ source('bronze_layer', 'binance') }}
)

SELECT
    *,
    current_timestamp as processed_at
FROM source_data
