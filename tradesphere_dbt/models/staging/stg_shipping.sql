-- Staging model for shipping
-- Casts dates to TIMESTAMP and calculates shipping_duration_days
-- Source: RAW.RAW_ORDERS

WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDERS') }}
),

shipping AS (
    SELECT
        "Order_Id"                                          AS order_id,
        "Shipping_Mode"                                     AS shipping_mode,
        "Delivery_Status"                                   AS delivery_status,
        TRY_CAST("Late_delivery_risk" AS INT)               AS late_delivery_risk,
        TRY_CAST("Days_for_shipping_real" AS INT)           AS days_shipping_real,
        TRY_CAST("Days_for_shipment_scheduled" AS INT)      AS days_shipping_scheduled,
        TRY_CAST("Days_for_shipping_real" AS INT) -
        TRY_CAST("Days_for_shipment_scheduled" AS INT)      AS shipping_delay_days,
        TRY_CAST("shipping_date_DateOrders" AS TIMESTAMP)   AS shipping_date,
        TRY_CAST("order_date_DateOrders" AS TIMESTAMP)      AS order_date
    FROM source
)

SELECT * FROM shipping
