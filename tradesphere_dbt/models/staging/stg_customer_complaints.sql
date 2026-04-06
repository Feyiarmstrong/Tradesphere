
-- Staging model for customer complaints
-- Casts booleans and dates, adds resolved_days
-- Source: RAW.RAW_COMPLAINTS loaded from Silver Delta Lake

WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_COMPLAINTS') }}
),

renamed AS (
    SELECT
        "complaint_id"                              AS complaint_id,
        "customer_id"                               AS customer_id,
        TRY_CAST("complaint_date" AS DATE)          AS complaint_date,
        "complaint_type"                            AS complaint_type,
        CASE
            WHEN UPPER("resolved") = 'TRUE' THEN TRUE
            ELSE FALSE
        END                                         AS resolved,
        "processed_at"                              AS processed_at
    FROM source
)

SELECT * FROM renamed
