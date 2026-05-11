{{
    config(
        materialized='table'
    )
}}

/*
    Lowercase + trim, then keep current rows only (Current flag = '1').
    Per PRD §5.3: no region standardisation — NSP is EMI's source of truth.

    Column rename strategy: quoted source names (with spaces) → snake_case
    output names so downstream models can reference them without quoting.

    Coordinates: source has NZTM easting/northing (NZ Transverse Mercator),
    NOT WGS84 lat/lng. Reverse-projection to lat/lng requires pyproj and is
    out of Phase 2 scope; kept as integers for any future mapping work.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_nsp') }}
)

SELECT
    TRIM("NSP")                                AS nsp_code,
    TRIM("POC code")                           AS poc_code,
    TRIM("Network participant")                AS network_participant,
    TRIM("Reconciliation type")                AS reconciliation_type,
    TRIM("Description")                        AS description,
    TRIM("Network reporting region")           AS region,
    TRIM("Network reporting region ID")        AS region_id,
    TRIM("Zone")                               AS zone,
    TRIM("Island")                             AS island,
    TRY_CAST(NULLIF("NZTM easting", '') AS INTEGER)   AS nztm_easting,
    TRY_CAST(NULLIF("NZTM northing", '') AS INTEGER)  AS nztm_northing,
    TRY_CAST(NULLIF("Start date", '') AS DATE)        AS start_date,
    TRY_CAST(NULLIF("End date", '') AS DATE)          AS end_date,
    _source_file_modified_at
FROM source
WHERE TRIM("Current flag") = '1'
  AND TRIM("POC code") IS NOT NULL
  AND TRIM("POC code") <> ''
