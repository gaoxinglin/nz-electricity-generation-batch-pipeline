{{
    config(
        materialized='table'
    )
}}

/*
    Raw → typed staging layer for Final Energy Prices.

    Source columns (4): trading_date, trading_period, point_of_connection,
    dollars_per_mwh — all VARCHAR. PRD §2.3 originally claimed 7 columns
    (Island, IsProxyPriceFlag, PublishDateTime); they do not exist in the
    actual EMI files as of 2026-05.

    Derived:
      - pricing_regime: 'ex_post' before var('pricing_regime_cutover'),
        'real_time' on/after — captures the 2022-11-01 NZ market change.
      - is_proxy: FALSE placeholder (source flag does not exist; column kept
        so int_price_daily/mart_price_* keep their `non_proxy` aggregates).

    Time zone: trading_date is NZ local (no TZ conversion); see PRD §5.3.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_price') }}
),

typed AS (
    SELECT
        CAST(trading_date AS DATE) AS trading_date,
        CAST(trading_period AS INTEGER) AS trading_period,
        point_of_connection AS poc_code,
        CAST(dollars_per_mwh AS DOUBLE) AS price_nzd_mwh,
        CASE
            WHEN CAST(trading_date AS DATE) < CAST('{{ var("pricing_regime_cutover") }}' AS DATE)
                THEN 'ex_post'
            ELSE 'real_time'
        END AS pricing_regime,
        FALSE AS is_proxy,
        trading_month,
        _source_file_modified_at
    FROM source
    WHERE trading_date IS NOT NULL
      AND trading_period IS NOT NULL
      AND point_of_connection IS NOT NULL
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY poc_code, trading_date, trading_period
            ORDER BY _source_file_modified_at DESC
        ) AS rn
    FROM typed
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['poc_code', 'trading_date', 'trading_period']) }}
        AS price_id,
    poc_code,
    trading_date,
    trading_period,
    price_nzd_mwh,
    pricing_regime,
    is_proxy,
    trading_month,
    _source_file_modified_at
FROM deduped
WHERE rn = 1
