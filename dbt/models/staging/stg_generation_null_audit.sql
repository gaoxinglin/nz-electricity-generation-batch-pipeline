{{
    config(
        materialized='view'
    )
}}

/*
    Captures TP1-TP46 NULLs only (anomalous).
    TP47-50 NULLs are expected (DST) and excluded.
*/

with source as (
    select * from {{ source('raw', 'raw_generation') }}
),

flattened as (
    select
        s.site_code,
        s.gen_code,
        s.fuel_code as raw_fuel_code,
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
            s.tp46
        )
    ) f
)

select
    site_code,
    gen_code,
    raw_fuel_code,
    trading_date,
    trading_month,
    trading_period,
    _source_file_modified_at
from flattened
where tp_value_raw is null
