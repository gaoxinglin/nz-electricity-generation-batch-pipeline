{{
    config(
        materialized='incremental',
        unique_key=['poc_code', 'trading_date', 'tp_number'],
        incremental_strategy='delete+insert'
    )
}}

/*
    Grain: poc_code × trading_date × tp_number.

    Two date columns (per PRD §5.3):
      - date_key INT (YYYYMMDD)  — surrogate for future BI joins to dim_date
      - trading_date DATE        — source col + driver of the incremental
                                   WHERE filter below

    Incremental strategy (per PRD §5.3 fct_price decision row):
      delete+insert on the natural key so interim → final price corrections
      overwrite cleanly. The lookback window is parameterised by
      var('lookback_days'); daily DAG run uses 3; the monthly final re-run
      uses 32 to fully cover the previous month.

    "From MAX(trading_date) backward" rather than CURRENT_DATE so backfills
    of historical months also re-process correctly.
*/

SELECT
    EXTRACT(YEAR FROM trading_date) * 10000
        + EXTRACT(MONTH FROM trading_date) * 100
        + EXTRACT(DAY FROM trading_date) AS date_key,
    trading_date,
    poc_code,
    trading_period AS tp_number,
    price_nzd_mwh,
    is_proxy,
    pricing_regime,
    _source_file_modified_at
FROM {{ ref('stg_price') }}

{% if is_incremental() %}
    WHERE trading_date >= (
        SELECT {{ dbt.dateadd('day', -var('lookback_days'), "COALESCE(MAX(trading_date), CAST('1900-01-01' AS DATE))") }}
        FROM {{ this }}
    )
{% endif %}
