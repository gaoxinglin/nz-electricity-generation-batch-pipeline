/*
    Cross-layer reconciliation: mart_renewable_ratio.total_kwh must equal
    the sum of all fuel types in mart_generation_monthly for each month.
    Returns rows if there is a mismatch (dbt test convention).
*/

with renewable as (
    select
        year_month,
        total_kwh as renewable_total_kwh
    from {{ ref('mart_renewable_ratio') }}
),

monthly as (
    select
        year_month,
        sum(total_generation_kwh) as monthly_total_kwh
    from {{ ref('mart_generation_monthly') }}
    group by year_month
)

select
    r.year_month,
    r.renewable_total_kwh,
    m.monthly_total_kwh,
    abs(r.renewable_total_kwh - m.monthly_total_kwh) as diff_kwh
from renewable r
inner join monthly m on r.year_month = m.year_month
where abs(r.renewable_total_kwh - m.monthly_total_kwh) > 1
