/*
    Reconciliation: fct_market_volume POC/TP rows must equal the distinct
    raw POC/date/TP count after staging dedupe. Returns rows on mismatch.
*/

WITH raw_grain AS (
    SELECT
        COUNT(*) AS raw_rows
    FROM (
        SELECT DISTINCT
            poc_code,
            trading_date,
            trading_period
        FROM {{ ref('stg_market_volume') }}
    )
),

fct_grain AS (
    SELECT COUNT(*) AS fct_rows
    FROM {{ ref('fct_market_volume') }}
)

SELECT
    raw_rows,
    fct_rows,
    raw_rows - fct_rows AS diff_rows
FROM raw_grain
CROSS JOIN fct_grain
WHERE raw_rows != fct_rows
