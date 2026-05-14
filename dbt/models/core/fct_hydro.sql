{{ config(materialized='table') }}

/*
    Daily hydro lake storage fact. One row per site × date.
    Grain: site_code × trading_date.

    Materialised as table (not incremental) — HMD is a periodic full snapshot,
    so every load replaces all rows in raw_hydro_storage. A full refresh here
    is therefore always correct and cheap (< 200k rows for 2016-2024 × 10 lakes).

    Joins dim_catchment for island/scheme metadata. LEFT JOIN so any site_code
    not yet in the seed (new release adds a lake) still appears in the output.
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.site_code', 's.trading_date']) }} AS hydro_id,
    CAST(
        CAST(EXTRACT(YEAR  FROM s.trading_date) AS INTEGER) * 10000
        + CAST(EXTRACT(MONTH FROM s.trading_date) AS INTEGER) * 100
        + CAST(EXTRACT(DAY   FROM s.trading_date) AS INTEGER)
    AS INTEGER)                            AS date_key,
    s.trading_date,
    s.site_code,
    c.catchment_name,
    c.island,
    c.plant_group,
    c.scheme,
    s.level_m,
    s.active_storage_mm3,
    s.contingent_storage_mm3,
    s.quality_code
FROM {{ ref('stg_hydro_storage') }} AS s
LEFT JOIN {{ ref('dim_catchment') }} AS c
    ON s.site_code = c.site_code
