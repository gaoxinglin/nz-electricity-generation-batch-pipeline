{{
    config(
        materialized='incremental',
        unique_key=['poc_code', 'trading_date', 'tp_number'],
        incremental_strategy='delete+insert'
    )
}}

/*
    POC x trading date x trading period volume fact.

    The reconciled source is participant-grain and has separate Injection and
    Offtake rows. This model collapses it to price-compatible POC/TP grain.
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_market_volume') }}

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
        trading_period AS tp_number,
        MAX(network) AS network,
        MAX(island) AS island,
        COUNT(DISTINCT participant) AS participant_count,
        SUM(CASE WHEN flow_direction = 'injection' THEN volume_kwh ELSE 0 END) AS injection_kwh,
        SUM(CASE WHEN flow_direction = 'offtake' THEN volume_kwh ELSE 0 END) AS offtake_kwh,
        MAX(_source_file_modified_at) AS _source_file_modified_at
    FROM source
    GROUP BY poc_code, trading_date, trading_period
)

SELECT
    EXTRACT(YEAR FROM trading_date) * 10000
        + EXTRACT(MONTH FROM trading_date) * 100
        + EXTRACT(DAY FROM trading_date) AS date_key,
    poc_code,
    trading_date,
    tp_number,
    network,
    island,
    participant_count,
    injection_kwh,
    offtake_kwh,
    injection_kwh - offtake_kwh AS net_injection_kwh,
    _source_file_modified_at
FROM aggregated
