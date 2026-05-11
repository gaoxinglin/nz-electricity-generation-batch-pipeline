{{
    config(
        materialized='incremental',
        unique_key='year_month',
        incremental_strategy='delete+insert'
    )
}}

/*
    Renewable = Hydro + Geothermal + Wind + Solar + Wood (is_renewable=true in fuel_codes seed).
    Battery excluded (storage, not generation).
    Sourced directly from fct_generation (not mart_generation_monthly) to keep DAG flat.
*/

SELECT
    trading_month AS year_month,
    SUM(generation_kwh) AS total_kwh,
    SUM(generation_kwh) / 1000000.0 AS total_gwh,
    SUM(CASE WHEN is_renewable THEN generation_kwh ELSE 0 END) AS renewable_kwh,
    SUM(CASE WHEN is_renewable THEN generation_kwh ELSE 0 END) / 1000000.0 AS renewable_gwh,
    ROUND(
        SUM(CASE WHEN is_renewable THEN generation_kwh ELSE 0 END) * 100.0
        / NULLIF(SUM(generation_kwh), 0),
        2
    ) AS renewable_pct
FROM {{ ref('fct_generation') }}

{% if is_incremental() %}
    WHERE trading_month >= (
        SELECT {{ yyyymm_minus_one_month('MAX(year_month)') }}
        FROM {{ this }}
    )
{% endif %}

GROUP BY trading_month
