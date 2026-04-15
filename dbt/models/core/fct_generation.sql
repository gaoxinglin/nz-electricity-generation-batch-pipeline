{{
    config(
        materialized='table'
    )
}}

/*
    One row per generator x trading period.
    generation_kwh in kWh (original unit).
    fuel_type is the transaction-time snapshot from stg_generation.
    Full rebuild every dbt run (~5M rows, ~5-10s on XS).
*/

select
    generation_id,
    {{ dbt_utils.generate_surrogate_key(['site_code', 'gen_code']) }} as plant_id,
    site_code,
    gen_code,
    fuel_type,
    is_renewable,
    trading_date,
    trading_month,
    trading_period,
    generation_kwh
from {{ ref('stg_generation') }}
