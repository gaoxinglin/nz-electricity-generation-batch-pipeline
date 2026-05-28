{{
    config(
        materialized='table'
    )
}}

/*
    Price anomaly events for market operations analysis.

    This expands the existing spike-only view with statistical context and
    reconciled demand/injection measures. It intentionally stays SQL-based;
    the ML feature mart can build on the same inputs without making anomaly
    monitoring depend on a Python model.

    Materialized as a full table so corrected prices can remove previously
    flagged anomalies. An incremental anomaly-only table would leave stale
    rows when a corrected price no longer breaches any anomaly rule.
*/

WITH price AS (
    SELECT
        poc_code,
        trading_date,
        tp_number,
        price_nzd_mwh,
        pricing_regime,
        is_proxy,
        {{ dbt.dateadd('minute', '(tp_number - 1) * 30', 'CAST(trading_date AS TIMESTAMP)') }}
            AS trading_period_start_ts
    FROM {{ ref('fct_price') }}
),

with_context AS (
    SELECT
        p.*,
        AVG(price_nzd_mwh) OVER (
            PARTITION BY poc_code
            ORDER BY trading_period_start_ts
            RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND INTERVAL '1 SECOND' PRECEDING
        ) AS rolling_30d_avg_price,
        STDDEV_SAMP(price_nzd_mwh) OVER (
            PARTITION BY poc_code
            ORDER BY trading_period_start_ts
            RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND INTERVAL '1 SECOND' PRECEDING
        ) AS rolling_30d_stddev_price
    FROM price AS p
),

classified AS (
    SELECT
        *,
        CASE
            WHEN price_nzd_mwh > {{ var('price_spike_threshold') }} THEN 'positive_spike'
            WHEN price_nzd_mwh < {{ var('negative_price_threshold') }} THEN 'negative_price'
            WHEN rolling_30d_stddev_price > 0
                AND ABS((price_nzd_mwh - rolling_30d_avg_price) / rolling_30d_stddev_price) >= 3
                THEN 'statistical_outlier'
        END AS anomaly_type,
        CASE
            WHEN rolling_30d_stddev_price > 0
                THEN (price_nzd_mwh - rolling_30d_avg_price) / rolling_30d_stddev_price
        END AS price_z_score
    FROM with_context
)

SELECT
    c.poc_code,
    c.trading_date,
    c.tp_number,
    c.price_nzd_mwh,
    c.pricing_regime,
    c.is_proxy,
    c.rolling_30d_avg_price,
    c.rolling_30d_stddev_price,
    c.price_z_score,
    c.anomaly_type,
    v.injection_kwh,
    v.offtake_kwh,
    v.net_injection_kwh,
    COALESCE(n.island, v.island) AS island,
    n.region,
    n.zone
FROM classified AS c
LEFT JOIN {{ ref('fct_market_volume') }} AS v
    ON c.poc_code = v.poc_code
    AND c.trading_date = v.trading_date
    AND c.tp_number = v.tp_number
LEFT JOIN {{ ref('dim_node') }} AS n
    ON c.poc_code = n.poc_code
WHERE c.anomaly_type IS NOT NULL
