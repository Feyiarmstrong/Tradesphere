
-- Date dimension table
-- Generates a row for every date in the orders range

WITH date_spine AS (
    SELECT
        DATEADD('day', SEQ4(), '2015-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)

SELECT
    date_day                                AS date_id,
    YEAR(date_day)                          AS year,
    MONTH(date_day)                         AS month,
    DAY(date_day)                           AS day,
    QUARTER(date_day)                       AS quarter,
    WEEKOFYEAR(date_day)                    AS week_of_year,
    DAYOFWEEK(date_day)                     AS day_of_week,
    DAYNAME(date_day)                       AS day_name,
    MONTHNAME(date_day)                     AS month_name,
    CASE WHEN DAYOFWEEK(date_day) IN (0, 6)
        THEN TRUE ELSE FALSE
    END                                     AS is_weekend
FROM date_spine
