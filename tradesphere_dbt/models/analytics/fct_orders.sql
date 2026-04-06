-- Main fact table for orders
-- Joined to all dimensions with currency conversion

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
),

stores AS (
    SELECT * FROM {{ ref('stg_store_regions') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_region,
    o.order_country,
    o.order_city,
    o.market,
    o.sales,
    o.order_quantity,
    o.discount,
    o.profit,
    o.order_value_usd,
    o.order_value_gbp,
    o.order_value_eur,
    o.order_date,
    o.order_month,
    o.order_quarter,
    o.order_year,
    o.is_weekend,
    o.delivery_status,
    o.late_delivery_risk,
    o.shipping_mode,
    c.customer_segment,
    c.lifetime_value_usd
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
