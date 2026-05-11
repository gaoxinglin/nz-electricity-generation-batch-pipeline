{{
    config(
        materialized='table'
    )
}}

/*
    Fact: one row per (invocation_id, node_unique_id).

    Derived columns:
      - is_success: status in ('pass', 'success') — distinguishes failure/skip
      - generated_date: the invocation's calendar day, for daily aggregation
      - invocation_seq: invocation rank by generated_at within node, lets the
        dashboard show "last N runs" without window-funky SQL each time

    Built as a table (not incremental) because the underlying volume is tiny
    (~150 rows per dbt invocation × handful of invocations per day) and a
    full rebuild is sub-second.
*/

WITH base AS (
    SELECT
        invocation_id,
        generated_at,
        CAST(generated_at AS DATE) AS generated_date,
        dbt_version,
        node_unique_id,
        node_type,
        node_name,
        status,
        execution_time_seconds,
        failures,
        message,
        relation_name,
        adapter_message,
        status IN ('pass', 'success') AS is_success
    FROM {{ ref('stg_dbt_run') }}
)

SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY node_unique_id
        ORDER BY generated_at DESC
    ) AS invocation_rank_desc
FROM base
