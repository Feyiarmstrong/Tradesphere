-- Staging model for exchange rates
-- Reads directly from RAW_ORDERS currency conversion columns
-- In future this will parse JSON from S3 exchange rates API

WITH orders AS (
    SELECT DISTINCT
        "processed_at"                          AS rate_date,
        TRY_CAST("order_value_usd" AS FLOAT) /
            NULLIF(TRY_CAST("Sales" AS FLOAT), 0) AS usd_rate,
        TRY_CAST("order_value_gbp" AS FLOAT) /
            NULLIF(TRY_CAST("Sales" AS FLOAT), 0) AS gbp_rate,
        TRY_CAST("order_value_eur" AS FLOAT) /
            NULLIF(TRY_CAST("Sales" AS FLOAT), 0) AS eur_rate
    FROM {{ source('raw', 'RAW_ORDERS') }}
    WHERE "Sales" IS NOT NULL
)

SELECT * FROM orders
