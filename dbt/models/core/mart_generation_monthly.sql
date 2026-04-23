{{
    config(
        materialized='incremental',
        unique_key=['year_month', 'fuel_type'],
        incremental_strategy='merge'
    )
}}

select
    trading_month as year_month,
    fuel_type,
    sum(generation_kwh) as total_generation_kwh,
    sum(generation_kwh) / 1000000.0 as total_generation_gwh,
    count(distinct gen_code) as generator_count,
    count(distinct trading_date) as active_days
from {{ ref('fct_generation') }}

{% if is_incremental() %}
where trading_month >= (
    select to_char(dateadd(month, -1, to_date(max(year_month) || '01', 'YYYYMMDD')), 'YYYYMM')
    from {{ this }}
)
{% endif %}

group by trading_month, fuel_type
