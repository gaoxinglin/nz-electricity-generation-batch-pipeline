{{
    config(
        materialized='table'
    )
}}

/*
    Type 1 SCD: one row per (site_code, gen_code).
    Reflects the LATEST snapshot — if a plant changes fuel type,
    historical analysis should use fct_generation.fuel_type instead.
*/

WITH latest AS (
    SELECT
        site_code,
        gen_code,
        poc_code,
        nwk_code,
        fuel_type,
        tech_code,
        ROW_NUMBER() OVER (
            PARTITION BY site_code, gen_code
            ORDER BY _source_file_modified_at DESC, trading_date DESC
        ) AS rn
    FROM {{ ref('stg_generation') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['site_code', 'gen_code']) }} AS plant_id,
    site_code,
    gen_code,
    poc_code,
    nwk_code,
    fuel_type,
    tech_code
FROM latest
WHERE rn = 1
