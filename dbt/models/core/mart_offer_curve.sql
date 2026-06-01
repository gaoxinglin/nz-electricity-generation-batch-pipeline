{{
    config(
        materialized='table'
    )
}}

/*
    POC x trading date x trading period supply-curve summary.

    These features compress the very large daily Offers source into a compact
    table that can be joined to price, volume, Streamlit, Power BI, or later ML
    experiments.

    Materialized as a full table to stay consistent with fct_offer_stack when a
    corrected daily Offers file removes rows from the raw source.
*/

WITH source AS (
    SELECT * FROM {{ ref('fct_offer_stack') }}
),

aggregated AS (
    SELECT
        poc_code,
        trading_date,
        tp_number,
        COUNT(*) AS offer_tranche_count,
        COUNT(DISTINCT participant_code) AS offer_participant_count,
        COUNT(DISTINCT unit) AS offer_unit_count,
        SUM(offer_mw) AS total_offered_mw,
        SUM(CASE WHEN offer_price_nzd_mwh <= 0 THEN offer_mw ELSE 0 END) AS offered_mw_at_or_below_0,
        SUM(CASE WHEN offer_price_nzd_mwh <= 100 THEN offer_mw ELSE 0 END) AS offered_mw_at_or_below_100,
        SUM(CASE WHEN offer_price_nzd_mwh <= 300 THEN offer_mw ELSE 0 END) AS offered_mw_at_or_below_300,
        SUM(CASE WHEN offer_price_nzd_mwh <= 500 THEN offer_mw ELSE 0 END) AS offered_mw_at_or_below_500,
        MIN(offer_price_nzd_mwh) AS min_offer_price_nzd_mwh,
        MAX(offer_price_nzd_mwh) AS max_offer_price_nzd_mwh,
        AVG(offer_price_nzd_mwh) AS avg_offer_price_nzd_mwh,
        CASE
            WHEN SUM(offer_mw) <> 0
                THEN SUM(offer_mw * offer_price_nzd_mwh) / SUM(offer_mw)
        END AS weighted_avg_offer_price_nzd_mwh,
        MAX(cumulative_offered_mw) AS max_cumulative_offered_mw,
        MAX(_source_file_modified_at) AS _source_file_modified_at
    FROM source
    GROUP BY poc_code, trading_date, tp_number
)

SELECT
    EXTRACT(YEAR FROM trading_date) * 10000
        + EXTRACT(MONTH FROM trading_date) * 100
        + EXTRACT(DAY FROM trading_date) AS date_key,
    poc_code,
    trading_date,
    tp_number,
    offer_tranche_count,
    offer_participant_count,
    offer_unit_count,
    total_offered_mw,
    offered_mw_at_or_below_0,
    offered_mw_at_or_below_100,
    offered_mw_at_or_below_300,
    offered_mw_at_or_below_500,
    min_offer_price_nzd_mwh,
    max_offer_price_nzd_mwh,
    avg_offer_price_nzd_mwh,
    weighted_avg_offer_price_nzd_mwh,
    max_cumulative_offered_mw,
    CASE
        WHEN total_offered_mw <> 0 THEN offered_mw_at_or_below_300 / total_offered_mw
    END AS cheap_offer_share_below_300,
    _source_file_modified_at
FROM aggregated
