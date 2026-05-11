{{
    config(
        materialized='table',
        enabled=(target.type == 'snowflake')
    )
}}

/*
    Tier-2 cost observability mart.

    Pulls from Snowflake's built-in ACCOUNT_USAGE views:
      - WAREHOUSE_METERING_HISTORY → credits consumed per warehouse per hour
      - QUERY_HISTORY              → query count + failure count per day

    Grain: day × warehouse. Joined onto a credit-cost reference for an
    indicative $/credit (Snowflake Standard tier defaults to ~US$2; PRD
    can override via var('snowflake_usd_per_credit')).

    Disabled on DuckDB (no ACCOUNT_USAGE equivalent). Pipeline Health page
    only renders the cost panel when target.type='snowflake'.

    ACCOUNT_USAGE has up to 45-minute latency by Snowflake's own docs, so
    intra-day numbers are approximate.
*/

WITH metering AS (
    SELECT
        CAST(start_time AS DATE) AS usage_date,
        warehouse_name,
        SUM(credits_used)               AS credits_used,
        SUM(credits_used_compute)       AS credits_used_compute,
        SUM(credits_used_cloud_services) AS credits_used_cloud_services
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(DAY, -90, CURRENT_DATE())
    GROUP BY 1, 2
),

queries AS (
    SELECT
        CAST(start_time AS DATE) AS usage_date,
        warehouse_name,
        COUNT(*) AS total_queries,
        SUM(CASE WHEN execution_status <> 'SUCCESS' THEN 1 ELSE 0 END) AS failed_queries,
        AVG(total_elapsed_time) / 1000.0 AS avg_query_seconds
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD(DAY, -90, CURRENT_DATE())
      AND warehouse_name IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    m.usage_date,
    m.warehouse_name,
    m.credits_used,
    m.credits_used_compute,
    m.credits_used_cloud_services,
    m.credits_used * {{ var('snowflake_usd_per_credit', 2.0) }} AS usd_estimated,
    COALESCE(q.total_queries, 0)  AS total_queries,
    COALESCE(q.failed_queries, 0) AS failed_queries,
    q.avg_query_seconds
FROM metering m
LEFT JOIN queries q
    ON m.usage_date = q.usage_date
    AND m.warehouse_name = q.warehouse_name
