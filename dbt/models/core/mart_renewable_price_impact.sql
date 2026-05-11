{{
    config(
        materialized='incremental',
        unique_key=['poc_code', 'trading_date', 'tp_number'],
        incremental_strategy='delete+insert'
    )
}}

/*
    Per (POC × date × tp): the renewable share of generation at that POC
    paired with the wholesale price at that POC and TP. Source for the
    "Renewable vs Price" dashboard page.

    Restricted to POCs that actually generate (INNER JOIN to
    int_generation_by_poc) — pure load POCs would have NULL renewable
    share and noise up the regression.

    Materialised as incremental on the same lookback window as fct_price
    so daily DAG runs propagate price corrections through.
*/

WITH gen_share AS (
    SELECT
        poc_code,
        trading_date,
        tp_number,
        SUM(generation_kwh) AS total_generation_kwh,
        SUM(CASE WHEN is_renewable THEN generation_kwh ELSE 0 END) AS renewable_generation_kwh
    FROM {{ ref('int_generation_by_poc') }}
    GROUP BY poc_code, trading_date, tp_number
),

priced AS (
    SELECT
        p.poc_code,
        p.trading_date,
        p.tp_number,
        p.price_nzd_mwh,
        p.pricing_regime,
        g.total_generation_kwh,
        g.renewable_generation_kwh,
        CASE
            WHEN g.total_generation_kwh > 0
                THEN 100.0 * g.renewable_generation_kwh / g.total_generation_kwh
        END AS renewable_pct
    FROM {{ ref('fct_price') }} AS p
    INNER JOIN gen_share AS g
        ON p.poc_code = g.poc_code
        AND p.trading_date = g.trading_date
        AND p.tp_number = g.tp_number

    {% if is_incremental() %}
        WHERE p.trading_date >= (
            SELECT {{ dbt.dateadd('day', -var('lookback_days'), "COALESCE(MAX(trading_date), CAST('1900-01-01' AS DATE))") }}
            FROM {{ this }}
        )
    {% endif %}
)

SELECT
    p.poc_code,
    p.trading_date,
    p.tp_number,
    p.price_nzd_mwh,
    p.pricing_regime,
    p.total_generation_kwh,
    p.renewable_generation_kwh,
    p.renewable_pct,
    n.island,
    n.region
FROM priced AS p
LEFT JOIN {{ ref('dim_node') }} AS n
    ON p.poc_code = n.poc_code
