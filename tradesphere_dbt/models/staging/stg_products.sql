-- Staging model for products
-- Extracted from orders table since no separate products source
-- Source: RAW.RAW_ORDERS

WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDERS') }}
),

products AS (
    SELECT DISTINCT
        "Product_Card_Id"                       AS product_id,
        "Product_Name"                          AS product_name,
        "Category_Name"                         AS category_name,
        "Department_Name"                       AS department_name,
        TRY_CAST("Product_Price" AS FLOAT)      AS product_price,
        "Product_Status"                        AS product_status
    FROM source
)

SELECT * FROM products