{{
    config(
        materialized='table'
    )
}}

/*
    Typed daily Offers staging model.

    The raw source contains all products, classes, submissions, and tranches.
    For price-context analytics we keep only the latest Energy/Injection offer
    rows, because those describe the supply stack most directly and keep the
    downstream feature tables much smaller than the raw daily files.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_offers') }}
),

typed AS (
    SELECT
        participant_code,
        point_of_connection AS poc_code,
        unit,
        LOWER(product_type) AS product_type,
        LOWER(product_class) AS product_class,
        reserve_type,
        product_description,
        CAST(trading_date AS DATE) AS trading_date,
        CAST(trading_period AS INTEGER) AS trading_period,
        CAST(utc_submission_date AS DATE) AS utc_submission_date,
        utc_submission_time,
        CAST(submission_order AS INTEGER) AS submission_order,
        UPPER(COALESCE(is_latest_yes_no, '')) = 'Y' AS is_latest,
        CAST(tranche AS INTEGER) AS tranche,
        CAST(NULLIF(maximum_ramp_up_mw_per_hour, '') AS DOUBLE) AS maximum_ramp_up_mw_per_hour,
        CAST(NULLIF(maximum_ramp_down_mw_per_hour, '') AS DOUBLE) AS maximum_ramp_down_mw_per_hour,
        CAST(NULLIF(partially_loaded_spinning_reserve_percent, '') AS DOUBLE)
            AS partially_loaded_spinning_reserve_percent,
        CAST(NULLIF(maximum_output_mw, '') AS DOUBLE) AS maximum_output_mw,
        CAST(NULLIF(forecast_generation_potential_mw, '') AS DOUBLE)
            AS forecast_generation_potential_mw,
        CAST(megawatts AS DOUBLE) AS offer_mw,
        CAST(dollars_per_mwh AS DOUBLE) AS offer_price_nzd_mwh,
        REPLACE(SUBSTR(CAST(CAST(trading_date AS DATE) AS VARCHAR), 1, 7), '-', '') AS trading_month,
        _source_file_modified_at
    FROM source
    WHERE trading_date IS NOT NULL
      AND trading_period IS NOT NULL
      AND participant_code IS NOT NULL
      AND point_of_connection IS NOT NULL
      AND unit IS NOT NULL
      AND tranche IS NOT NULL
      AND megawatts IS NOT NULL
      AND dollars_per_mwh IS NOT NULL
),

latest_energy_injection AS (
    SELECT *
    FROM typed
    WHERE product_type = 'energy'
      AND product_class = 'injection'
      AND is_latest
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY participant_code, poc_code, unit, trading_date, trading_period, tranche
            ORDER BY submission_order DESC, _source_file_modified_at DESC
        ) AS rn
    FROM latest_energy_injection
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'participant_code',
        'poc_code',
        'unit',
        'trading_date',
        'trading_period',
        'tranche'
    ]) }} AS offer_id,
    participant_code,
    poc_code,
    unit,
    product_type,
    product_class,
    reserve_type,
    product_description,
    trading_date,
    trading_period,
    utc_submission_date,
    utc_submission_time,
    submission_order,
    is_latest,
    tranche,
    maximum_ramp_up_mw_per_hour,
    maximum_ramp_down_mw_per_hour,
    partially_loaded_spinning_reserve_percent,
    maximum_output_mw,
    forecast_generation_potential_mw,
    offer_mw,
    offer_price_nzd_mwh,
    trading_month,
    _source_file_modified_at
FROM deduped
WHERE rn = 1
