-- Staging model for customers
-- Extracted from orders table since no separate customers source
-- Source: RAW.RAW_ORDERS

WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDERS') }}
),

customers AS (
    SELECT DISTINCT
        "Order_Customer_Id"                     AS customer_id,
        "Customer_Fname"                        AS first_name,
        "Customer_Lname"                        AS last_name,
        "Customer_Email"                        AS email,
        "Customer_Segment"                      AS customer_segment,
        "Customer_City"                         AS city,
        "Customer_State"                        AS state,
        "Customer_Country"                      AS country,
        "Customer_Zipcode"                      AS zipcode
    FROM source
)

SELECT * FROM customers
