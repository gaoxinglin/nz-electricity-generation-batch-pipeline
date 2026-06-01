WITH staging AS (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('stg_energy_offer') }}
),

fact AS (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('fct_offer_stack') }}
)

SELECT
    staging.row_count AS staging_rows,
    fact.row_count AS fact_rows
FROM staging
CROSS JOIN fact
WHERE staging.row_count <> fact.row_count
