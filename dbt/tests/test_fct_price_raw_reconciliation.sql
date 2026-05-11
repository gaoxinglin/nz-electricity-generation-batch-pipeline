/*
    Reconciliation: fct_price row count must equal the deduplicated row count
    in raw_price keyed by (poc_code, trading_date, trading_period). dbt test
    convention: returns rows on mismatch.
*/

with raw_distinct as (
    select count(*) as raw_rows
    from (
        select distinct point_of_connection, trading_date, trading_period
        from {{ source('raw', 'raw_price') }}
    )
),

fct_count as (
    select count(*) as fct_rows
    from {{ ref('fct_price') }}
)

select raw_rows, fct_rows, raw_rows - fct_rows as diff_rows
from raw_distinct
cross join fct_count
where raw_rows != fct_rows
