{{
    config(
        materialized='incremental',
        unique_key=['trading_date', 'fuel_type'],
        incremental_strategy='merge'
    )
}}

select
    trading_date,
    fuel_type,
    sum(generation_kwh) as total_generation_kwh,
    sum(generation_kwh) / 1000000.0 as total_generation_gwh,
    count(distinct gen_code) as generator_count
from {{ ref('fct_generation') }}

{% if is_incremental() %}
where trading_date >= (select dateadd(day, -3, max(trading_date)) from {{ this }})
{% endif %}

group by trading_date, fuel_type
