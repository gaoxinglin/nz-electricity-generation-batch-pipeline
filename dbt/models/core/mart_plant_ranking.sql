{{
    config(
        materialized='incremental',
        unique_key=['year_month', 'site_code'],
        incremental_strategy='merge'
    )
}}

WITH monthly_generation AS (
    SELECT
        trading_month AS year_month,
        site_code,
        fuel_type,
        SUM(generation_kwh) AS fuel_generation_kwh
    FROM {{ ref('fct_generation') }}

    {% if is_incremental() %}
        WHERE trading_month >= (
            SELECT TO_CHAR(DATEADD(MONTH, -1, TO_DATE(MAX(year_month) || '01', 'YYYYMMDD')), 'YYYYMM')  -- noqa: RF02
            FROM {{ this }}
        )
    {% endif %}

    GROUP BY trading_month, site_code, fuel_type
),

site_totals AS (
    SELECT
        year_month,
        site_code,
        SUM(fuel_generation_kwh) AS total_generation_kwh,
        SUM(fuel_generation_kwh) / 1000000.0 AS total_generation_gwh
    FROM monthly_generation
    GROUP BY year_month, site_code
),

primary_fuel AS (
    SELECT
        year_month,
        site_code,
        fuel_type AS primary_fuel_type,
        ROW_NUMBER() OVER (
            PARTITION BY year_month, site_code
            ORDER BY fuel_generation_kwh DESC
        ) AS rn
    FROM monthly_generation
)

SELECT
    st.year_month,
    st.site_code,
    st.total_generation_kwh,
    st.total_generation_gwh,
    pf.primary_fuel_type,
    RANK() OVER (
        PARTITION BY st.year_month
        ORDER BY st.total_generation_kwh DESC
    ) AS monthly_rank
FROM site_totals AS st
INNER JOIN primary_fuel AS pf
    ON
        st.year_month = pf.year_month
        AND st.site_code = pf.site_code
        AND pf.rn = 1
