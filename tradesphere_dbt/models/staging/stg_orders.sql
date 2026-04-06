-- Staging model for orders
-- Casts types, renames columns, adds time dimensions
-- Source: RAW.RAW_ORDERS loaded from Silver Delta Lake

WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDERS') }}
),

renamed AS (
    SELECT
        "Order_Id"                          AS order_id,
        "Order_Customer_Id"                 AS customer_id,
        "Order_Status"                      AS order_status,
        "Order_Region"                      AS order_region,
        "Order_Country"                     AS order_country,
        "Order_City"                        AS order_city,
        "Order_State"                       AS order_state,
        "Market"                            AS market,
        TRY_CAST("Sales" AS FLOAT)          AS sales,
        TRY_CAST("Order_Item_Quantity" AS INT) AS order_quantity,
        TRY_CAST("Order_Item_Discount" AS FLOAT) AS discount,
        TRY_CAST("Order_Profit_Per_Order" AS FLOAT) AS profit,
        TRY_CAST("Benefit_per_order" AS FLOAT) AS benefit_per_order,
        TRY_CAST("order_value_usd" AS FLOAT) AS order_value_usd,
        TRY_CAST("order_value_gbp" AS FLOAT) AS order_value_gbp,
        TRY_CAST("order_value_eur" AS FLOAT) AS order_value_eur,
        TRY_CAST("order_date_DateOrders" AS TIMESTAMP) AS order_date,
        TRY_CAST("order_month" AS INT)      AS order_month,
        TRY_CAST("order_quarter" AS INT)    AS order_quarter,
        TRY_CAST("order_year" AS INT)       AS order_year,
        "is_weekend"                        AS is_weekend,
        "Delivery_Status"                   AS delivery_status,
        TRY_CAST("Late_delivery_risk" AS INT) AS late_delivery_risk,
        "Shipping_Mode"                     AS shipping_mode,
        "processed_at"                      AS processed_at
    FROM source
)

SELECT * FROM renamed