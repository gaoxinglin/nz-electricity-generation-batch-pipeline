{{
    config(
        materialized='view'
    )
}}

/*
    Outlier audit: positive spikes (> price_spike_threshold) and negative
    prices (< negative_price_threshold). NZ wholesale typically sits at
    $50-150/MWh; thresholds are dbt vars so they can be tuned without code.

    Materialized as a view because this is monitoring-only — no downstream
    model joins it, and we want fresh reads against stg_price without an
    extra rebuild step.
*/

SELECT
    poc_code,
    trading_date,
    trading_period,
    price_nzd_mwh,
    pricing_regime,
    CASE
        WHEN price_nzd_mwh > {{ var('price_spike_threshold') }} THEN 'positive_spike'
        WHEN price_nzd_mwh < {{ var('negative_price_threshold') }} THEN 'negative_price'
    END AS outlier_type
FROM {{ ref('stg_price') }}
WHERE price_nzd_mwh > {{ var('price_spike_threshold') }}
   OR price_nzd_mwh < {{ var('negative_price_threshold') }}
