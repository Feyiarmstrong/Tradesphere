-- Dimension table for stores
-- Includes region grouping

WITH stores AS (
    SELECT * FROM {{ ref('stg_store_regions') }}
)

SELECT
    store_id,
    store_name,
    city,
    country,
    region,
    manager_name,
    open_date,
    DATEDIFF('year', open_date, CURRENT_DATE) AS years_open
FROM stores
