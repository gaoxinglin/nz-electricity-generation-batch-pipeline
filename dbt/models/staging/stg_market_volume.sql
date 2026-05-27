{{
    config(
        materialized='table'
    )
}}

/*
    Typed reconciled injection/offtake volumes.

    Grain: POC x participant x trading date x trading period x flow
    direction. FlowDirection is normalised to lowercase so the fact layer
    can pivot Injection/Offtake into measure columns.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_market_volume') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'point_of_connection',
        'participant',
        'trading_date',
        'trading_period',
        'flow_direction'
    ]) }} AS market_volume_id,
    point_of_connection AS poc_code,
    network,
    island,
    participant,
    CAST(trading_date AS DATE) AS trading_date,
    CAST(trading_period AS INTEGER) AS trading_period,
    trading_period_start_time,
    LOWER(flow_direction) AS flow_direction,
    CAST(kilowatt_hours AS DOUBLE) AS volume_kwh,
    REPLACE(SUBSTR(CAST(CAST(trading_date AS DATE) AS VARCHAR), 1, 7), '-', '') AS trading_month,
    _source_file_modified_at
FROM source
WHERE point_of_connection IS NOT NULL
  AND trading_date IS NOT NULL
  AND trading_period IS NOT NULL
  AND flow_direction IS NOT NULL
  AND kilowatt_hours IS NOT NULL
