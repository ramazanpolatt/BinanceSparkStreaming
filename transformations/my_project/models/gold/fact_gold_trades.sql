{{ config(
    materialized='table'
) }}

SELECT
    symbol,
    DATE_TRUNC('hour', window_start) as trade_hour,
    ROUND(AVG(price),2) as avg_hourly_price,
    ROUND(SUM(quantity),3) as total_hourly_volume,
    MAX(price) as high_price,
    MIN(price) as low_price
FROM {{ ref('fact_silver_trades') }}
GROUP BY 1, 2
ORDER BY 2 DESC, 1 ASC