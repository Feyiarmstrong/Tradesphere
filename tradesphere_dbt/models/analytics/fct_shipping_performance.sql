-- Fact table for shipping performance
-- Includes on-time delivery rate metrics

WITH shipping AS (
    SELECT * FROM {{ ref('stg_shipping') }}
)

SELECT
    order_id,
    shipping_mode,
    delivery_status,
    late_delivery_risk,
    days_shipping_real,
    days_shipping_scheduled,
    shipping_delay_days,
    shipping_date,
    order_date,
    CASE
        WHEN delivery_status = 'Shipping on time' THEN TRUE
        ELSE FALSE
    END AS is_on_time
FROM shipping
