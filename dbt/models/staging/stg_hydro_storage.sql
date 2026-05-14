{{ config(materialized='table') }}

/*
    Typed hydro lake storage. Source = raw.raw_hydro_storage, loaded by
    scripts/load_local.py (local) / COPY INTO (cloud).

    Filters to 2016-01-01 to align with the pipeline's unified start date.
    NULL active_storage rows are dropped (data gaps in pre-1990 records).
    Quality codes are kept as-is (200/310/320/350 are all valid EMI codes).
*/

SELECT
    site_code,
    TRY_CAST(date_str AS DATE)                   AS trading_date,
    TRY_CAST(level_m AS FLOAT)                   AS level_m,
    TRY_CAST(active_storage_mm3 AS FLOAT)        AS active_storage_mm3,
    TRY_CAST(contingent_storage_mm3 AS FLOAT)    AS contingent_storage_mm3,
    TRY_CAST(quality_code AS INTEGER)            AS quality_code
FROM {{ source('raw', 'raw_hydro_storage') }}
WHERE date_str IS NOT NULL
  AND TRY_CAST(date_str AS DATE) IS NOT NULL
  AND TRY_CAST(date_str AS DATE) >= CAST('2016-01-01' AS DATE)
  AND TRY_CAST(active_storage_mm3 AS FLOAT) IS NOT NULL
