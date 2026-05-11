/*
    SUM(generation_kwh) in int_generation_by_poc must equal SUM(generation_kwh)
    in stg_generation when restricted to rows with non-null poc_code.
    Returns rows on mismatch.
*/

with stg_total as (
    select sum(generation_kwh) as stg_kwh
    from {{ ref('stg_generation') }}
    where poc_code is not null
),

int_total as (
    select sum(generation_kwh) as int_kwh
    from {{ ref('int_generation_by_poc') }}
)

select stg_kwh, int_kwh, stg_kwh - int_kwh as diff_kwh
from stg_total
cross join int_total
where stg_kwh != int_kwh
