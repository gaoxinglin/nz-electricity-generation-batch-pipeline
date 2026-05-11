{{
    config(
        materialized='incremental',
        unique_key=['poc_code', 'trading_date'],
        incremental_strategy='delete+insert'
    )
}}

/*
    Daily price summary at POC grain, joined to dim_node for island/region
    attributes. Covers PRD's Q6 (POC daily price) directly, and Q9 (island
    price spread) via GROUP BY island in dashboard SQL.

    Two parallel aggregates:
      avg_price_*       — across all 48 (±DST) TPs
      avg_price_non_proxy — same, excluding proxy rows (currently identical
                            because is_proxy=FALSE in source; column kept
                            for forward compatibility when EMI re-publishes
                            the flag).

    Incremental: same lookback strategy as fct_price so the same daily DAG
    refresh window covers downstream marts without re-fingering the var.
*/

WITH price AS (
    SELECT * FROM {{ ref('fct_price') }}

    {% if is_incremental() %}
        WHERE trading_date >= (
            SELECT {{ dbt.dateadd('day', -var('lookback_days'), "COALESCE(MAX(trading_date), CAST('1900-01-01' AS DATE))") }}
            FROM {{ this }}
        )
    {% endif %}
),

aggregated AS (
    SELECT
        poc_code,
        trading_date,
        COUNT(*) AS tp_count,
        AVG(price_nzd_mwh) AS avg_price_all,
        AVG(CASE WHEN NOT is_proxy THEN price_nzd_mwh END) AS avg_price_non_proxy,
        MIN(price_nzd_mwh) AS min_price,
        MAX(price_nzd_mwh) AS max_price,
        STDDEV_SAMP(price_nzd_mwh) AS stddev_price,
        SUM(CASE WHEN price_nzd_mwh > {{ var('price_spike_threshold') }} THEN 1 ELSE 0 END) AS spike_tp_count,
        SUM(CASE WHEN price_nzd_mwh < {{ var('negative_price_threshold') }} THEN 1 ELSE 0 END) AS negative_tp_count,
        MAX(pricing_regime) AS pricing_regime
    FROM price
    GROUP BY poc_code, trading_date
)

SELECT
    EXTRACT(YEAR FROM a.trading_date) * 10000
        + EXTRACT(MONTH FROM a.trading_date) * 100
        + EXTRACT(DAY FROM a.trading_date) AS date_key,
    a.trading_date,
    a.poc_code,
    n.island,
    n.region,
    n.zone,
    n.network_participant,
    a.tp_count,
    a.avg_price_all,
    a.avg_price_non_proxy,
    a.min_price,
    a.max_price,
    a.stddev_price,
    a.spike_tp_count,
    a.negative_tp_count,
    a.pricing_regime
FROM aggregated AS a
LEFT JOIN {{ ref('dim_node') }} AS n
    ON a.poc_code = n.poc_code
