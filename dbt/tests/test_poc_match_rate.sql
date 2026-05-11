/*
    POC match-rate gate (PRD §5.3): the share of distinct fct_price POCs
    that exist in dim_node should be high. Generation POCs are a subset of
    price POCs (load POCs have prices but no generation), so the natural
    comparison is fct_price ↔ dim_node, not generation ↔ price.

    Warn if matched < 80% (NSP is missing too many price POCs);
    error if matched < 50% (something is fundamentally wrong).
    Configured here at warn level — a separate explicit dbt_test should
    upgrade to error for production.
*/

with price_pocs as (
    select distinct poc_code from {{ ref('fct_price') }}
),

matched as (
    select count(distinct p.poc_code) as matched_pocs
    from price_pocs as p
    inner join {{ ref('dim_node') }} as n
        on p.poc_code = n.poc_code
),

totals as (
    select count(*) as price_poc_count
    from price_pocs
)

select
    matched_pocs,
    price_poc_count,
    1.0 * matched_pocs / price_poc_count as match_rate
from matched
cross join totals
where 1.0 * matched_pocs / price_poc_count < 0.80
