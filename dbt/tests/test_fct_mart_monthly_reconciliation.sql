/*
    Cross-layer reconciliation: SUM(generation_kwh) in fct_generation
    must equal SUM(total_generation_kwh) in mart_generation_monthly.
    Returns rows if there is a mismatch (dbt test convention).
*/

with fct_total as (
    select sum(generation_kwh) as fct_kwh
    from {{ ref('fct_generation') }}
),

mart_total as (
    select sum(total_generation_kwh) as mart_kwh
    from {{ ref('mart_generation_monthly') }}
)

select
    fct_kwh,
    mart_kwh,
    abs(fct_kwh - mart_kwh) as diff_kwh
from fct_total
cross join mart_total
where abs(fct_kwh - mart_kwh) > 1
