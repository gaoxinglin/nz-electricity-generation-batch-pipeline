{{
    config(
        materialized='table'
    )
}}

/*
    Type 1 dimension at POC grain.

    A POC can have multiple NSP rows in stg_nsp (e.g. when the network
    participant has changed over time, even with Current flag = 1). We pick
    the row with the latest start_date; ties broken by nsp_code asc for
    determinism.

    Coordinates kept as NZTM (NZ Transverse Mercator). PRD §2.3 said
    "lat/lng" but actual source publishes NZTM; reverse-projection deferred.
*/

WITH ranked AS (
    SELECT
        poc_code,
        nsp_code,
        network_participant,
        reconciliation_type,
        region,
        region_id,
        zone,
        island,
        nztm_easting,
        nztm_northing,
        ROW_NUMBER() OVER (
            PARTITION BY poc_code
            ORDER BY COALESCE(start_date, DATE '1900-01-01') DESC, nsp_code ASC
        ) AS rn
    FROM {{ ref('stg_nsp') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['poc_code']) }} AS node_id,
    poc_code,
    nsp_code,
    network_participant,
    reconciliation_type,
    region,
    region_id,
    zone,
    island,
    nztm_easting,
    nztm_northing
FROM ranked
WHERE rn = 1
