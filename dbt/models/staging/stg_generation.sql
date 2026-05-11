{{
    config(
        materialized='table'
    )
}}

/*
    Output schema unchanged from V1 — only the FLATTEN-vs-UNNEST machinery
    is now factored into the unpivot_trading_periods macro so the model
    compiles on both Snowflake and DuckDB.

    The macro is called with TP1-TP50 to cover NZ DST (spring-forward day
    has 46 TPs, autumn-back day has 50; missing TPs arrive as NULL and are
    dropped by the macro's WHERE).
*/

{% set tp_cols = [] %}
{% for i in range(1, 51) %}
    {% do tp_cols.append({'index': i, 'col': 'tp' ~ i}) %}
{% endfor %}

WITH unpivoted AS (
    {{ unpivot_trading_periods(
         source('raw', 'raw_generation'),
         tp_cols,
         [
             'site_code', 'poc_code', 'nwk_code', 'gen_code',
             'fuel_code AS raw_fuel_code', 'tech_code',
             'trading_date', 'trading_month', '_source_file_modified_at'
         ],
         value_col_name='tp_value_raw'
       ) }}
),

with_fuel AS (
    SELECT
        u.site_code,
        u.poc_code,
        u.nwk_code,
        u.gen_code,
        u.raw_fuel_code,
        fc.fuel_type,
        fc.is_renewable,
        u.tech_code,
        CAST(u.trading_date AS DATE) AS trading_date,
        u.trading_month,
        u.tp_number AS trading_period,
        CAST(u.tp_value_raw AS INTEGER) AS generation_kwh,
        u._source_file_modified_at
    FROM unpivoted AS u
    INNER JOIN {{ ref('fuel_codes') }} AS fc
        ON u.raw_fuel_code = fc.raw_fuel_code
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trading_date, trading_period, site_code, gen_code
            ORDER BY _source_file_modified_at DESC
        ) AS rn
    FROM with_fuel
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['site_code', 'gen_code', 'trading_date', 'trading_period']) }}
        AS generation_id,
    site_code,
    poc_code,
    nwk_code,
    gen_code,
    raw_fuel_code,
    fuel_type,
    is_renewable,
    tech_code,
    trading_date,
    trading_month,
    trading_period,
    generation_kwh,
    _source_file_modified_at
FROM deduped
WHERE rn = 1
