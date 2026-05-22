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
),

normalised AS (
    {% if target.type == 'snowflake' %}
    SELECT
        current_flag,
        nsp,
        poc_code,
        network_participant,
        reconciliation_type,
        description,
        network_reporting_region,
        network_reporting_region_id,
        zone,
        island,
        nztm_easting,
        nztm_northing,
        start_date,
        end_date,
        _source_file_modified_at
    FROM source
    {% else %}
    SELECT
        "Current flag" AS current_flag,
        "NSP" AS nsp,
        "POC code" AS poc_code,
        "Network participant" AS network_participant,
        "Reconciliation type" AS reconciliation_type,
        "Description" AS description,
        "Network reporting region" AS network_reporting_region,
        "Network reporting region ID" AS network_reporting_region_id,
        "Zone" AS zone,
        "Island" AS island,
        "NZTM easting" AS nztm_easting,
        "NZTM northing" AS nztm_northing,
        "Start date" AS start_date,
        "End date" AS end_date,
        _source_file_modified_at
    FROM source
    {% endif %}
)

SELECT
    TRIM(nsp)                                      AS nsp_code,
    TRIM(poc_code)                                 AS poc_code,
    TRIM(network_participant)                      AS network_participant,
    TRIM(reconciliation_type)                      AS reconciliation_type,
    TRIM(description)                              AS description,
    TRIM(network_reporting_region)                 AS region,
    TRIM(network_reporting_region_id)              AS region_id,
    TRIM(zone)                                     AS zone,
    TRIM(island)                                   AS island,
    TRY_CAST(NULLIF(nztm_easting, '') AS INTEGER)  AS nztm_easting,
    TRY_CAST(NULLIF(nztm_northing, '') AS INTEGER) AS nztm_northing,
    TRY_CAST(NULLIF(start_date, '') AS DATE)       AS start_date,
    TRY_CAST(NULLIF(end_date, '') AS DATE)         AS end_date,
    _source_file_modified_at
FROM normalised
WHERE TRIM(current_flag) = '1'
  AND TRIM(poc_code) IS NOT NULL
  AND TRIM(poc_code) <> ''
