/*
    Data completeness: row count per trading_month should be
    between 5,000 and 300,000 in stg_generation.
    Returns rows that breach the threshold.
*/

select
    trading_month,
    count(*) as row_count
from {{ ref('stg_generation') }}
group by trading_month
having count(*) < 5000 or count(*) > 300000
