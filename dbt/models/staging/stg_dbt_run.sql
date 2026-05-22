{{
    config(
        materialized='view'
    )
}}

/*
    Typed staging on top of raw.raw_dbt_run. All-time history — observability
    needs trends, so we don't aggressively prune. Materialised as view (small
    table, no need to rebuild).
*/

SELECT
    "invocation_id" AS invocation_id,
    CAST("generated_at" AS TIMESTAMP) AS generated_at,
    "dbt_version" AS dbt_version,
    "node_unique_id" AS node_unique_id,
    "node_type" AS node_type,
    "node_name" AS node_name,
    "status" AS status,
    CAST(NULLIF("execution_time_seconds", '') AS DOUBLE) AS execution_time_seconds,
    CAST(NULLIF("failures", '') AS INTEGER) AS failures,
    NULLIF("message", '')        AS message,
    NULLIF("relation_name", '')  AS relation_name,
    NULLIF("adapter_message", '') AS adapter_message
FROM {{ source('raw', 'raw_dbt_run') }}
