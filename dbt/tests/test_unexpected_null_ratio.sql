/*
    Warn if TP1-46 NULL ratio > 0.1% of total rows in stg_generation.
    TP47-50 NULLs are expected (DST) and are NOT counted here.
    Returns rows if the threshold is breached (dbt test convention).
*/

{{ config(severity='warn') }}

with total_rows as (
    select count(*) as total_count
    from {{ ref('stg_generation') }}
),

null_count as (
    select count(*) as null_count
    from {{ ref('stg_generation_null_audit') }}
)

select
    nc.null_count,
    tr.total_count,
    round(nc.null_count * 100.0 / nullif(tr.total_count, 0), 4) as null_pct
from null_count nc
cross join total_rows tr
where nc.null_count > tr.total_count * 0.001
