{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    Grain: poc_code × trading_date × tp_number  (per PRD §5.2 / §5.3).

    Despite the "_daily" suffix, the row grain is TP-level — this matches
    fct_generation's grain so mart_price_spike_events can join on
    (poc_code, trading_date, tp_number) without first aggregating.

    Daily aggregates are carried as window functions on each TP row:
      - avg_price_all       : daily mean across all 48 (±DST) TPs at this POC
      - avg_price_non_proxy : daily mean excluding proxy rows (currently
                              identical to avg_price_all because is_proxy
                              is always FALSE; column reserved for when the
                              proxy flag returns to EMI source files).
*/

SELECT
    poc_code,
    trading_date,
    tp_number,
    price_nzd_mwh,
    is_proxy,
    pricing_regime,
    AVG(price_nzd_mwh) OVER (
        PARTITION BY poc_code, trading_date
    ) AS avg_price_all,
    AVG(CASE WHEN NOT is_proxy THEN price_nzd_mwh END) OVER (
        PARTITION BY poc_code, trading_date
    ) AS avg_price_non_proxy
FROM {{ ref('fct_price') }}
