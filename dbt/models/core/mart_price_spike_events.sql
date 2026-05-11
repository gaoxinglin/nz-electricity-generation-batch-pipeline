{{
    config(
        materialized='incremental',
        unique_key=['poc_code', 'trading_date', 'tp_number'],
        incremental_strategy='delete+insert'
    )
}}

/*
    One row per spike event = (POC × date × tp) where price exceeds
    price_spike_threshold. LEFT JOIN to int_generation_by_poc so each
    spike row carries the fuel-mix snapshot that was generating at that
    POC during the spike TP — the central feature for Q7/Q8.

    `unmatched_generation` flags spike rows where the generation side has
    no matching POC entry (either retired POC or load-only POC). Dashboard
    surfaces this as the "join coverage" indicator (PRD §5.3 POC match
    rate gate).

    Multi-fuel fanout: int_generation_by_poc is grain (POC × date × tp ×
    fuel); SUM/STRING_AGG below collapses back to spike grain.
*/

WITH spike_prices AS (
    SELECT
        poc_code,
        trading_date,
        tp_number,
        price_nzd_mwh,
        pricing_regime,
        is_proxy
    FROM {{ ref('fct_price') }}
    WHERE price_nzd_mwh > {{ var('price_spike_threshold') }}

    {% if is_incremental() %}
        AND trading_date >= (
            SELECT {{ dbt.dateadd('day', -var('lookback_days'), "COALESCE(MAX(trading_date), CAST('1900-01-01' AS DATE))") }}
            FROM {{ this }}
        )
    {% endif %}
),

gen_at_spike AS (
    SELECT
        poc_code,
        trading_date,
        tp_number,
        SUM(generation_kwh) AS total_generation_kwh,
        SUM(CASE WHEN is_renewable THEN generation_kwh ELSE 0 END) AS renewable_generation_kwh,
        SUM(CASE WHEN NOT is_renewable THEN generation_kwh ELSE 0 END) AS thermal_generation_kwh
    FROM {{ ref('int_generation_by_poc') }}
    GROUP BY poc_code, trading_date, tp_number
)

SELECT
    s.poc_code,
    s.trading_date,
    s.tp_number,
    s.price_nzd_mwh,
    s.pricing_regime,
    s.is_proxy,
    n.island,
    n.region,
    g.total_generation_kwh,
    g.renewable_generation_kwh,
    g.thermal_generation_kwh,
    CASE
        WHEN g.poc_code IS NULL THEN TRUE
        ELSE FALSE
    END AS unmatched_generation
FROM spike_prices AS s
LEFT JOIN gen_at_spike AS g
    ON s.poc_code = g.poc_code
    AND s.trading_date = g.trading_date
    AND s.tp_number = g.tp_number
LEFT JOIN {{ ref('dim_node') }} AS n
    ON s.poc_code = n.poc_code
