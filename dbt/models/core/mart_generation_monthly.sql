{{
    config(
        materialized='incremental',
        unique_key=['year_month', 'fuel_type'],
        incremental_strategy='delete+insert'
    )
}}

SELECT
    trading_month AS year_month,
    fuel_type,
    SUM(generation_kwh) AS total_generation_kwh,
    SUM(generation_kwh) / 1000000.0 AS total_generation_gwh,
    COUNT(DISTINCT gen_code) AS generator_count,
    COUNT(DISTINCT trading_date) AS active_days
FROM {{ ref('fct_generation') }}

{% if is_incremental() %}
    WHERE trading_month >= (
        SELECT {{ yyyymm_minus_one_month('MAX(year_month)') }}
        FROM {{ this }}
    )
{% endif %}

GROUP BY trading_month, fuel_type
