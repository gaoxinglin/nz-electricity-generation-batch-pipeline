{{
    config(
        materialized='table'
    )
}}

/*
    Canonical fuel dimension at fuel_type grain (one row per distinct
    fuel_type), built on top of the fuel_codes seed which holds raw → type
    mappings. Use this when joining marts that want a clean fuel-type list
    (avoids fan-out from multiple raw aliases like Gas / Gas&Oil).
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(['fuel_type']) }} AS fuel_id,
    fuel_type,
    MAX(CASE WHEN is_renewable THEN 1 ELSE 0 END) = 1 AS is_renewable
FROM {{ ref('fuel_codes') }}
GROUP BY fuel_type
