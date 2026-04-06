-- Staging model for store regions
-- Cleans store metadata and standardises region names
-- Source: RAW.RAW_STORE_REGIONS loaded from Silver Delta Lake

WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_STORE_REGIONS') }}
),

renamed AS (
    SELECT
        "store_id"                              AS store_id,
        "store_name"                            AS store_name,
        "city"                                  AS city,
        "country"                               AS country,
        UPPER(TRIM("region"))                   AS region,
        "manager_name"                          AS manager_name,
        TRY_CAST("open_date" AS DATE)           AS open_date,
        "processed_at"                          AS processed_at
    FROM source
)

SELECT * FROM renamed
