{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ source('raw', 'raw_generation') }}
),

flattened as (
    select
        s.site_code,
        s.poc_code,
        s.nwk_code,
        s.gen_code,
        s.fuel_code as raw_fuel_code,
        s.tech_code,
        s.trading_date::date as trading_date,
        s.trading_month,
        s._source_file_modified_at,
        f.index + 1 as trading_period,
        nullif(f.value::string, '') as tp_value_raw
    from source s,
    lateral flatten(
        input => array_construct(
            s.tp1,  s.tp2,  s.tp3,  s.tp4,  s.tp5,
            s.tp6,  s.tp7,  s.tp8,  s.tp9,  s.tp10,
            s.tp11, s.tp12, s.tp13, s.tp14, s.tp15,
            s.tp16, s.tp17, s.tp18, s.tp19, s.tp20,
            s.tp21, s.tp22, s.tp23, s.tp24, s.tp25,
            s.tp26, s.tp27, s.tp28, s.tp29, s.tp30,
            s.tp31, s.tp32, s.tp33, s.tp34, s.tp35,
            s.tp36, s.tp37, s.tp38, s.tp39, s.tp40,
            s.tp41, s.tp42, s.tp43, s.tp44, s.tp45,
            s.tp46, s.tp47, s.tp48, s.tp49, s.tp50
        )
    ) f
),

with_fuel as (
    select
        fl.site_code,
        fl.poc_code,
        fl.nwk_code,
        fl.gen_code,
        fl.raw_fuel_code,
        fc.fuel_type,
        fc.is_renewable,
        fl.tech_code,
        fl.trading_date,
        fl.trading_month,
        fl.trading_period,
        fl.tp_value_raw::integer as generation_kwh,
        fl._source_file_modified_at
    from flattened fl
    inner join {{ ref('fuel_codes') }} fc
        on fl.raw_fuel_code = fc.raw_fuel_code
    where fl.tp_value_raw is not null
),

deduped as (
    select
        *,
        row_number() over (
            partition by trading_date, trading_period, site_code, gen_code
            order by _source_file_modified_at desc
        ) as rn
    from with_fuel
)

select
    {{ dbt_utils.generate_surrogate_key(['site_code', 'gen_code', 'trading_date', 'trading_period']) }} as generation_id,
    site_code,
    poc_code,
    nwk_code,
    gen_code,
    raw_fuel_code,
    fuel_type,
    is_renewable,
    tech_code,
    trading_date,
    trading_month,
    trading_period,
    generation_kwh,
    _source_file_modified_at
from deduped
where rn = 1
