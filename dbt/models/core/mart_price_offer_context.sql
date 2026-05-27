{{
    config(
        materialized='table'
    )
}}

/*
    Price rows enriched with offer-curve and reconciled volume context.
    This is designed for BI drill-down: "was a high price accompanied by a
    thin local offer stack?"
*/

WITH price AS (
    SELECT * FROM {{ ref('fct_price') }}
),

volume AS (
    SELECT * FROM {{ ref('fct_market_volume') }}
),

offer_curve AS (
    SELECT * FROM {{ ref('mart_offer_curve') }}
)

SELECT
    p.date_key,
    p.poc_code,
    p.trading_date,
    p.tp_number,
    p.price_nzd_mwh,
    p.pricing_regime,
    p.is_proxy,
    v.injection_kwh,
    v.offtake_kwh,
    v.net_injection_kwh,
    v.injection_kwh / 500.0 AS injection_mw,
    v.offtake_kwh / 500.0 AS offtake_mw,
    oc.offer_tranche_count,
    oc.offer_participant_count,
    oc.offer_unit_count,
    oc.total_offered_mw,
    oc.offered_mw_at_or_below_0,
    oc.offered_mw_at_or_below_100,
    oc.offered_mw_at_or_below_300,
    oc.offered_mw_at_or_below_500,
    oc.min_offer_price_nzd_mwh,
    oc.max_offer_price_nzd_mwh,
    oc.avg_offer_price_nzd_mwh,
    oc.weighted_avg_offer_price_nzd_mwh,
    oc.cheap_offer_share_below_300,
    CASE
        WHEN v.offtake_kwh <> 0 THEN oc.total_offered_mw / (v.offtake_kwh / 500.0)
    END AS offered_to_offtake_ratio
FROM price AS p
LEFT JOIN volume AS v
    ON p.poc_code = v.poc_code
    AND p.trading_date = v.trading_date
    AND p.tp_number = v.tp_number
LEFT JOIN offer_curve AS oc
    ON p.poc_code = oc.poc_code
    AND p.trading_date = oc.trading_date
    AND p.tp_number = oc.tp_number
