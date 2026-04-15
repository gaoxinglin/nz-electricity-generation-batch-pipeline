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

with with_season as (
    select
        generation_kwh,
        fuel_type,
        trading_date,
        extract(month from trading_date) as month_num,
        extract(year from trading_date) as cal_year,
        case
            when extract(month from trading_date) in (12, 1, 2) then 'Summer'
            when extract(month from trading_date) in (3, 4, 5) then 'Autumn'
            when extract(month from trading_date) in (6, 7, 8) then 'Winter'
            when extract(month from trading_date) in (9, 10, 11) then 'Spring'
        end as season,
        -- Dec belongs to next year's season
        case
            when extract(month from trading_date) = 12
            then extract(year from trading_date) + 1
            else extract(year from trading_date)
        end as season_year
    from {{ ref('fct_generation') }}
)

select
    season_year,
    season,
    fuel_type,
    sum(generation_kwh) / 1000000.0 as total_generation_gwh,
    avg(generation_kwh) / 1000000.0 as avg_generation_gwh
from with_season
group by season_year, season, fuel_type
