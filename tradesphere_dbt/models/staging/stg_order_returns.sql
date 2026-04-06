-- Staging model for order returns
-- Cleans returns data and calculates days_to_return
-- Source: RAW.RAW_RETURNS loaded from Silver Delta Lake

WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_RETURNS') }}
),

renamed AS (
    SELECT
        "return_id"                             AS return_id,
        "order_id"                              AS order_id,
        TRY_CAST("return_date" AS DATE)         AS return_date,
        "return_reason"                         AS return_reason,
        TRY_CAST("refund_amount" AS FLOAT)      AS refund_amount,
        TRY_CAST("days_to_return" AS INT)       AS days_to_return,
        "processed_at"                          AS processed_at
    FROM source
)

SELECT * FROM renamed