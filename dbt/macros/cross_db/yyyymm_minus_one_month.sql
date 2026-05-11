{#
    yyyymm_minus_one_month — given a YYYYMM string expression, return the
    YYYYMM string for one calendar month earlier. Used by V1 monthly marts'
    incremental WHERE clauses; kept here so SF and DuckDB stay in lock-step.
#}
{% macro yyyymm_minus_one_month(ym_expr) %}
  {% if target.type == 'snowflake' %}
    TO_CHAR(DATEADD(MONTH, -1, TO_DATE({{ ym_expr }} || '01', 'YYYYMMDD')), 'YYYYMM')
  {% elif target.type == 'duckdb' %}
    strftime(strptime({{ ym_expr }} || '01', '%Y%m%d') - INTERVAL 1 MONTH, '%Y%m')
  {% else %}
    {{ exceptions.raise_compiler_error("yyyymm_minus_one_month: unsupported target " ~ target.type) }}
  {% endif %}
{% endmacro %}
