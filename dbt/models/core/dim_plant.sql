{{
    config(
        materialized='table'
    )
}}

/*
    Type 1 SCD: one row per (site_code, gen_code).
    Reflects the LATEST snapshot — if a plant changes fuel type,
    historical analysis should use fct_generation.fuel_type instead.
*/

with latest as (
    select
        site_code,
        gen_code,
        poc_code,
        nwk_code,
        fuel_type,
        tech_code,
        row_number() over (
            partition by site_code, gen_code
            order by _source_file_modified_at desc, trading_date desc
        ) as rn
    from {{ ref('stg_generation') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['site_code', 'gen_code']) }} as plant_id,
    site_code,
    gen_code,
    poc_code,
    nwk_code,
    fuel_type,
    tech_code
from latest
where rn = 1
