{{
    config(
        materialized='incremental',
        unique_key=['year_month', 'site_code'],
        incremental_strategy='merge'
    )
}}

with monthly_generation as (
    select
        trading_month as year_month,
        site_code,
        fuel_type,
        sum(generation_kwh) as fuel_generation_kwh
    from {{ ref('fct_generation') }}

    {% if is_incremental() %}
    where trading_month >= (
        select to_char(dateadd(month, -1, to_date(max(year_month) || '01', 'YYYYMMDD')), 'YYYYMM')
        from {{ this }}
    )
    {% endif %}

    group by trading_month, site_code, fuel_type
),

site_totals as (
    select
        year_month,
        site_code,
        sum(fuel_generation_kwh) as total_generation_kwh,
        sum(fuel_generation_kwh) / 1000000.0 as total_generation_gwh
    from monthly_generation
    group by year_month, site_code
),

primary_fuel as (
    select
        year_month,
        site_code,
        fuel_type as primary_fuel_type,
        row_number() over (
            partition by year_month, site_code
            order by fuel_generation_kwh desc
        ) as rn
    from monthly_generation
)

select
    st.year_month,
    st.site_code,
    st.total_generation_kwh,
    st.total_generation_gwh,
    pf.primary_fuel_type,
    rank() over (
        partition by st.year_month
        order by st.total_generation_kwh desc
    ) as monthly_rank
from site_totals st
inner join primary_fuel pf
    on st.year_month = pf.year_month
    and st.site_code = pf.site_code
    and pf.rn = 1
