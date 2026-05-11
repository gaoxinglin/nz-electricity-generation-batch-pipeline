{{
    config(
        materialized='view'
    )
}}

/*
    Captures TP1-TP46 NULLs only (anomalous).
    TP47-50 NULLs are expected (DST autumn-back has 50; spring-forward has 46)
    and excluded.

    Cannot reuse unpivot_trading_periods macro because the macro filters
    `val IS NOT NULL` — this audit needs the inverse. We keep the unpivot
    dialect-switch inline; TP range is hard-capped at 46.
*/

WITH unpivoted AS (
    {% if target.type == 'snowflake' %}
        SELECT
            site_code,
            gen_code,
            fuel_code AS raw_fuel_code,
            trading_date,
            trading_month,
            _source_file_modified_at,
            f.value:tp::INT AS trading_period,
            f.value:val::STRING AS tp_value_raw
        FROM {{ source('raw', 'raw_generation') }},
        LATERAL FLATTEN(
            input => ARRAY_CONSTRUCT(
                {%- for i in range(1, 47) -%}
                    OBJECT_CONSTRUCT('tp', {{ i }}, 'val', tp{{ i }})
                    {%- if not loop.last %}, {% endif -%}
                {%- endfor -%}
            )
        ) AS f
    {% elif target.type == 'duckdb' %}
        SELECT
            site_code,
            gen_code,
            fuel_code AS raw_fuel_code,
            trading_date,
            trading_month,
            _source_file_modified_at,
            unnested.tp AS trading_period,
            unnested.val AS tp_value_raw
        FROM {{ source('raw', 'raw_generation') }},
        UNNEST([
            {%- for i in range(1, 47) -%}
                {'tp': {{ i }}, 'val': tp{{ i }}}
                {%- if not loop.last %}, {% endif -%}
            {%- endfor -%}
        ]) AS _u(unnested)
    {% endif %}
)

SELECT
    site_code,
    gen_code,
    raw_fuel_code,
    CAST(trading_date AS DATE) AS trading_date,
    trading_month,
    trading_period,
    _source_file_modified_at
FROM unpivoted
WHERE tp_value_raw IS NULL OR tp_value_raw = ''
