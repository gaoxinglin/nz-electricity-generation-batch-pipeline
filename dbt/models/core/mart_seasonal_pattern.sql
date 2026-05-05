{{
    config(
        materialized='table'
    )
}}

/*
    NZ meteorological seasons:
      Summer: Dec-Feb  (December → next year's season_year)
      Autumn: Mar-May
      Winter: Jun-Aug
      Spring: Sep-Nov

    Full refresh — no incremental (season spans multiple months).
*/

WITH with_season AS (
    SELECT
        generation_kwh,
        fuel_type,
        trading_date,
        EXTRACT(MONTH FROM trading_date) AS month_num,
        EXTRACT(YEAR FROM trading_date) AS cal_year,
        CASE
            WHEN EXTRACT(MONTH FROM trading_date) IN (12, 1, 2) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM trading_date) IN (3, 4, 5) THEN 'Autumn'
            WHEN EXTRACT(MONTH FROM trading_date) IN (6, 7, 8) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM trading_date) IN (9, 10, 11) THEN 'Spring'
        END AS season,
        -- Dec belongs to next year's season
        CASE
            WHEN EXTRACT(MONTH FROM trading_date) = 12
                THEN EXTRACT(YEAR FROM trading_date) + 1
            ELSE EXTRACT(YEAR FROM trading_date)
        END AS season_year
    FROM {{ ref('fct_generation') }}
)

SELECT
    season_year,
    season,
    fuel_type,
    SUM(generation_kwh) / 1000000.0 AS total_generation_gwh,
    AVG(generation_kwh) / 1000000.0 AS avg_generation_gwh
FROM with_season
GROUP BY season_year, season, fuel_type
