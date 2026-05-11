{{
    config(
        materialized='table'
    )
}}

/*
    Plant-level grain in fct_generation can fan out a POC across multiple
    rows per (date, tp) when multiple plants share a POC. This intermediate
    SUMs to POC × date × tp × fuel so price-joined marts (spike events,
    renewable impact) get a 1-row-per-key surface without re-doing the
    aggregation inline.

    Materialised as a table (PRD §5.2) since it's an upstream of multiple
    marts; a view would re-aggregate per mart run.
*/

SELECT
    poc_code,
    trading_date,
    trading_period AS tp_number,
    fuel_type,
    is_renewable,
    SUM(generation_kwh) AS generation_kwh
FROM {{ ref('stg_generation') }}
WHERE poc_code IS NOT NULL
GROUP BY poc_code, trading_date, trading_period, fuel_type, is_renewable
