{{
    config(
        materialized='incremental',
        unique_key='year_month',
        incremental_strategy='merge'
    )
}}

/*
    Renewable = Hydro + Geothermal + Wind + Solar + Wood (is_renewable=true in fuel_codes seed).
    Battery excluded (storage, not generation).
    Sourced directly from fct_generation (not mart_generation_monthly) to keep DAG flat.
*/

select
    trading_month as year_month,
    sum(generation_kwh) as total_kwh,
    sum(generation_kwh) / 1000000.0 as total_gwh,
    sum(case when is_renewable then generation_kwh else 0 end) as renewable_kwh,
    sum(case when is_renewable then generation_kwh else 0 end) / 1000000.0 as renewable_gwh,
    round(
        sum(case when is_renewable then generation_kwh else 0 end) * 100.0
        / nullif(sum(generation_kwh), 0),
        2
    ) as renewable_pct
from {{ ref('fct_generation') }}

{% if is_incremental() %}
where trading_month >= (
    select to_char(dateadd(month, -1, to_date(max(year_month) || '01', 'YYYYMMDD')), 'YYYYMM')
    from {{ this }}
)
{% endif %}

group by trading_month
