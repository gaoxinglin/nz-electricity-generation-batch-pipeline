{{
    config(
        materialized='table'
    )
}}

/*
    Feature table for price spike prediction experiments.

    Target: is_price_spike = price_nzd_mwh > var('price_spike_threshold').
    This mart is intentionally model-agnostic: Python, Snowpark ML, or a BI
    notebook can consume the same stable feature grain.

    Materialized as a full table so lag and rolling-window features are
    identical between scheduled runs and full refreshes.
*/

WITH price AS (
    SELECT * FROM {{ ref('fct_price') }}
),

gen_share AS (
    SELECT
        poc_code,
        trading_date,
        tp_number,
        SUM(generation_kwh) AS generation_kwh,
        SUM(CASE WHEN is_renewable THEN generation_kwh ELSE 0 END) AS renewable_generation_kwh,
        CASE
            WHEN SUM(generation_kwh) > 0
                THEN 100.0 * SUM(CASE WHEN is_renewable THEN generation_kwh ELSE 0 END) / SUM(generation_kwh)
        END AS renewable_pct
    FROM {{ ref('int_generation_by_poc') }}
    GROUP BY poc_code, trading_date, tp_number
),

base AS (
    SELECT
        p.poc_code,
        p.trading_date,
        p.tp_number,
        p.price_nzd_mwh,
        p.pricing_regime,
        p.is_proxy,
        v.injection_kwh,
        v.offtake_kwh,
        v.net_injection_kwh,
        g.generation_kwh,
        g.renewable_generation_kwh,
        g.renewable_pct,
        n.island,
        n.region,
        d.month,
        d.day_of_week_iso,
        d.is_weekend,
        d.is_nz_holiday,
        d.season
    FROM price AS p
    LEFT JOIN {{ ref('fct_market_volume') }} AS v
        ON p.poc_code = v.poc_code
        AND p.trading_date = v.trading_date
        AND p.tp_number = v.tp_number
    LEFT JOIN gen_share AS g
        ON p.poc_code = g.poc_code
        AND p.trading_date = g.trading_date
        AND p.tp_number = g.tp_number
    LEFT JOIN {{ ref('dim_node') }} AS n
        ON p.poc_code = n.poc_code
    LEFT JOIN {{ ref('dim_date') }} AS d
        ON p.trading_date = d.trading_date
),

features AS (
    SELECT
        *,
        LAG(price_nzd_mwh) OVER (
            PARTITION BY poc_code
            ORDER BY trading_date, tp_number
        ) AS previous_tp_price,
        AVG(price_nzd_mwh) OVER (
            PARTITION BY poc_code
            ORDER BY trading_date, tp_number
            ROWS BETWEEN 336 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_avg_price,
        STDDEV_SAMP(price_nzd_mwh) OVER (
            PARTITION BY poc_code
            ORDER BY trading_date, tp_number
            ROWS BETWEEN 336 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_stddev_price,
        AVG(offtake_kwh) OVER (
            PARTITION BY poc_code
            ORDER BY trading_date, tp_number
            ROWS BETWEEN 336 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_avg_offtake_kwh
    FROM base
)

SELECT
    poc_code,
    trading_date,
    tp_number,
    price_nzd_mwh,
    price_nzd_mwh > {{ var('price_spike_threshold') }} AS is_price_spike,
    pricing_regime,
    is_proxy,
    previous_tp_price,
    rolling_7d_avg_price,
    rolling_7d_stddev_price,
    injection_kwh,
    offtake_kwh,
    net_injection_kwh,
    rolling_7d_avg_offtake_kwh,
    generation_kwh,
    renewable_generation_kwh,
    renewable_pct,
    island,
    region,
    month,
    day_of_week_iso,
    is_weekend,
    is_nz_holiday,
    season
FROM features
