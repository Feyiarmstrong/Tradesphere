-- Dimension table for customers
-- Includes customer lifetime value calculation

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customer_stats AS (
    SELECT
        customer_id,
        COUNT(order_id)             AS total_orders,
        SUM(order_value_usd)        AS lifetime_value_usd,
        AVG(order_value_usd)        AS avg_order_value_usd,
        MIN(order_date)             AS first_order_date,
        MAX(order_date)             AS last_order_date
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.customer_segment,
    c.city,
    c.state,
    c.country,
    c.zipcode,
    cs.total_orders,
    cs.lifetime_value_usd,
    cs.avg_order_value_usd,
    cs.first_order_date,
    cs.last_order_date
FROM customers c
LEFT JOIN customer_stats cs ON c.customer_id = cs.customer_id
