{{ config(materialized='table') }}

/*
    Monthly hydro storage vs wholesale price per island.
    Grain: island × year_month (VARCHAR YYYYMM).

    Answers Q10: "When NI/SI hydro storage is low, are wholesale prices higher?"
    The storage_pct_of_max column expresses each month's average total storage
    as a percentage of the island's all-time historical maximum (within the
    loaded dataset), giving a normalised scarcity indicator.

    LEFT JOIN on price so hydro months without price data are retained —
    the dashboard handles the NULL price case gracefully.

    Coverage note: HMD data ends 2024-12-31. Full correlation analysis requires
    price data from the same period (make local-full or make local-subset).
*/

WITH island_daily AS (
    SELECT
        island,
        trading_date,
        SUM(active_storage_mm3)    AS total_storage_mm3,
        COUNT(DISTINCT site_code)  AS site_count
    FROM {{ ref('fct_hydro') }}
    WHERE island IS NOT NULL
    GROUP BY island, trading_date
),

hydro_monthly AS (
    SELECT
        island,
        CAST(
            CAST(EXTRACT(YEAR  FROM trading_date) AS INTEGER) * 100
            + CAST(EXTRACT(MONTH FROM trading_date) AS INTEGER)
        AS VARCHAR)                         AS year_month,
        AVG(total_storage_mm3)              AS avg_total_storage_mm3,
        MIN(total_storage_mm3)              AS min_storage_mm3,
        MAX(total_storage_mm3)              AS max_storage_mm3,
        COUNT(*)                            AS observation_days,
        CAST(AVG(CAST(site_count AS FLOAT)) AS FLOAT) AS avg_sites_reporting
    FROM island_daily
    GROUP BY island, year_month
),

hydro_with_pct AS (
    SELECT
        h.*,
        ROUND(
            100.0 * h.avg_total_storage_mm3
            / NULLIF(MAX(h.avg_total_storage_mm3) OVER (PARTITION BY h.island), 0),
            1
        ) AS storage_pct_of_max
    FROM hydro_monthly AS h
),

price_monthly AS (
    SELECT
        island,
        CAST(
            CAST(EXTRACT(YEAR  FROM trading_date) AS INTEGER) * 100
            + CAST(EXTRACT(MONTH FROM trading_date) AS INTEGER)
        AS VARCHAR)                              AS year_month,
        AVG(avg_price_all)                       AS avg_price_nzd_mwh,
        AVG(avg_price_non_proxy)                 AS avg_price_non_proxy,
        COUNT(DISTINCT trading_date)             AS price_days,
        COUNT(DISTINCT poc_code)                 AS poc_count
    FROM {{ ref('mart_price_daily') }}
    WHERE island IS NOT NULL
    GROUP BY island, year_month
)

SELECT
    h.island,
    h.year_month,
    h.avg_total_storage_mm3,
    h.min_storage_mm3,
    h.max_storage_mm3,
    h.storage_pct_of_max,
    h.observation_days,
    h.avg_sites_reporting,
    p.avg_price_nzd_mwh,
    p.avg_price_non_proxy,
    p.price_days,
    p.poc_count
FROM hydro_with_pct AS h
LEFT JOIN price_monthly AS p
    ON h.island = p.island
    AND h.year_month = p.year_month
ORDER BY h.island, h.year_month
