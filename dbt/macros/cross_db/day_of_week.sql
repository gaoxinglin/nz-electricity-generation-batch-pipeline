{#
    day_of_week — ISO-style 1=Monday … 7=Sunday across Snowflake and DuckDB.

    Why a wrapper: Snowflake DAYOFWEEK uses 0=Sunday by default (configurable
    via WEEK_START), while DuckDB ISODOW returns 1=Monday..7=Sunday natively.
    Forcing a single convention here removes the "did the session set
    WEEK_START?" footgun.
#}
{% macro day_of_week(date_col) %}
  {% if target.type == 'snowflake' %}
    DAYOFWEEKISO({{ date_col }})
  {% elif target.type == 'duckdb' %}
    ISODOW({{ date_col }})
  {% else %}
    {{ exceptions.raise_compiler_error("day_of_week: unsupported target " ~ target.type) }}
  {% endif %}
{% endmacro %}
