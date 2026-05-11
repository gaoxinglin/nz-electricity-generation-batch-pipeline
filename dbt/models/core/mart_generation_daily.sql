{{
    config(
        materialized='incremental',
        unique_key=['trading_date', 'fuel_type'],
        incremental_strategy='delete+insert'
    )
}}

SELECT
    trading_date,
    fuel_type,
    SUM(generation_kwh) AS total_generation_kwh,
    SUM(generation_kwh) / 1000000.0 AS total_generation_gwh,
    COUNT(DISTINCT gen_code) AS generator_count
FROM {{ ref('fct_generation') }}

{% if is_incremental() %}
    WHERE trading_date >= (SELECT {{ dbt.dateadd('day', -3, 'MAX(trading_date)') }} FROM {{ this }})  -- noqa: RF02
{% endif %}

GROUP BY trading_date, fuel_type
