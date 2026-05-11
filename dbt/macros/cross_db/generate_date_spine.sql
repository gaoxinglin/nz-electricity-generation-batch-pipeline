{#
    generate_date_spine — wraps dbt_utils.date_spine with a NZ-default
    "today's date in NZ" upper bound. Both Snowflake and DuckDB execute
    dbt_utils.date_spine via their native generators.

    Args:
      start_date — ISO date string ('YYYY-MM-DD').
      end_date   — ISO date string; defaults to (current_date + 1 day) so the
                   spine covers today.
      datepart   — granularity; default 'day'.

    Output column: date_day (the dbt_utils default).
#}
{% macro generate_date_spine(start_date, end_date=None, datepart='day') %}
  {% set _end = end_date if end_date else "dateadd('day', 1, current_date)" %}
  {{ dbt_utils.date_spine(
       datepart=datepart,
       start_date="cast('" ~ start_date ~ "' as date)",
       end_date=_end
  ) }}
{% endmacro %}
