{{
    config(
        materialized='table'
    )
}}

/*
    Latest Energy/Injection offer stack at POC x trading period x unit x tranche
    grain. The cumulative MW column gives BI and modelling consumers a simple
    supply-curve feature without scanning the raw daily Offers files.

    Materialized as a full table because daily offer files can be corrected by
    removing or de-latest-ing rows. Full rebuilds avoid stale offer_ids after a
    raw trading-day reload.
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_energy_offer') }}
)

SELECT
    offer_id,
    EXTRACT(YEAR FROM trading_date) * 10000
        + EXTRACT(MONTH FROM trading_date) * 100
        + EXTRACT(DAY FROM trading_date) AS date_key,
    participant_code,
    poc_code,
    unit,
    trading_date,
    trading_period AS tp_number,
    tranche,
    offer_mw,
    offer_price_nzd_mwh,
    offer_mw * offer_price_nzd_mwh AS offered_mw_price_value,
    SUM(offer_mw) OVER (
        PARTITION BY poc_code, trading_date, trading_period
        ORDER BY offer_price_nzd_mwh, tranche, participant_code, unit
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_offered_mw,
    maximum_ramp_up_mw_per_hour,
    maximum_ramp_down_mw_per_hour,
    maximum_output_mw,
    forecast_generation_potential_mw,
    _source_file_modified_at
FROM source
