-- Fact table for returns
-- Includes return rate metrics

WITH returns AS (
    SELECT * FROM {{ ref('stg_order_returns') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    r.return_id,
    r.order_id,
    r.return_date,
    r.return_reason,
    r.refund_amount,
    r.days_to_return,
    o.customer_id,
    o.order_value_usd,
    o.order_country,
    o.order_region,
    ROUND(r.refund_amount / NULLIF(o.order_value_usd, 0) * 100, 2) AS refund_rate_pct
FROM returns r
LEFT JOIN orders o ON r.order_id = o.order_id
