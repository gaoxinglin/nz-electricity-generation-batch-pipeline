{{
    config(
        materialized='table'
    )
}}

/*
    NZ-flavoured date dimension.

    Spans 2016-01-01 to 2030-12-31 (one row per day). Seasons follow NIWA's
    meteorological convention for the southern hemisphere (Summer Dec-Feb…),
    with `season_year` rolling December into the next year so Summer never
    splits across calendar years.

    `is_nz_holiday` LEFT JOINs the `nz_public_holidays` seed; holidays
    through 2030 are covered.
*/

WITH spine AS (
    {{ generate_date_spine(
         start_date='2016-01-01',
         end_date="cast('2030-12-31' as date)"
    ) }}
),

base AS (
    SELECT
        CAST(date_day AS DATE) AS trading_date,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        {{ day_of_week('date_day') }} AS day_of_week_iso
    FROM spine
)

SELECT
    EXTRACT(YEAR FROM trading_date) * 10000
        + EXTRACT(MONTH FROM trading_date) * 100
        + EXTRACT(DAY FROM trading_date) AS date_key,
    trading_date,
    year,
    quarter,
    month,
    day,
    day_of_week_iso,
    (day_of_week_iso IN (6, 7)) AS is_weekend,
    (h.holiday_date IS NOT NULL) AS is_nz_holiday,
    h.holiday_name,
    CASE
        WHEN month IN (12, 1, 2)  THEN 'Summer'
        WHEN month IN (3, 4, 5)   THEN 'Autumn'
        WHEN month IN (6, 7, 8)   THEN 'Winter'
        ELSE 'Spring'
    END AS season,
    CASE WHEN month = 12 THEN year + 1 ELSE year END AS season_year
FROM base
LEFT JOIN {{ ref('nz_public_holidays') }} AS h
    ON base.trading_date = h.holiday_date
