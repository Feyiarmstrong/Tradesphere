-- Dimension table for products
-- Includes product category hierarchy

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_id,
    product_name,
    category_name,
    department_name,
    product_price,
    product_status,
    CASE
        WHEN product_price < 50 THEN 'Budget'
        WHEN product_price BETWEEN 50 AND 200 THEN 'Mid-range'
        ELSE 'Premium'
    END AS price_tier
FROM products
