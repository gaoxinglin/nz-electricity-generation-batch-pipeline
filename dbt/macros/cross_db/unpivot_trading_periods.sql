{#
    unpivot_trading_periods
    ───────────────────────
    Pivot Generation_MD-style wide TP columns into long (tp_number, value) rows.

    Args:
      relation       — dbt ref()/source() expression for the wide source table.
      tp_columns     — list of dicts {index: int, col: str}. Must always cover
                       TP01-TP50 (50 entries) to handle NZ DST: spring-forward
                       day has only 46 TPs, autumn-back day has 50; missing TPs
                       arrive as NULL and are filtered out by WHERE.
      id_columns     — list of column names to pass through unchanged.
      value_col_name — alias of the unpivoted value column. Default: "value".

    Output: SELECT statement with id_columns + tp_number (INT) + value_col_name.
    The macro emits no surrounding CTE so callers can wrap it as needed.

    Both branches keep `value` as the source column's native type (raw_generation
    casts to VARCHAR upstream, so downstream stg layer ::INT casts safely).
#}
{% macro unpivot_trading_periods(relation, tp_columns, id_columns, value_col_name='value') %}
  {% if target.type == 'snowflake' %}
    SELECT
      {% for col in id_columns %}{{ col }}, {% endfor %}
      f.value:tp::INT AS tp_number,
      f.value:val::STRING AS {{ value_col_name }}
    FROM {{ relation }},
    LATERAL FLATTEN(
      input => ARRAY_CONSTRUCT(
        {% for tp in tp_columns -%}
          OBJECT_CONSTRUCT('tp', {{ tp.index }}, 'val', {{ tp.col }})
          {%- if not loop.last %}, {% endif %}
        {%- endfor %}
      )
    ) AS f
    WHERE f.value:val IS NOT NULL
  {% elif target.type == 'duckdb' %}
    SELECT
      {% for col in id_columns %}{{ col }}, {% endfor %}
      unnested.tp AS tp_number,
      unnested.val AS {{ value_col_name }}
    FROM {{ relation }},
    UNNEST([
      {% for tp in tp_columns -%}
        {'tp': {{ tp.index }}, 'val': {{ tp.col }}}
        {%- if not loop.last %}, {% endif %}
      {%- endfor %}
    ]) AS _u(unnested)
    WHERE unnested.val IS NOT NULL
  {% else %}
    {{ exceptions.raise_compiler_error("unpivot_trading_periods: unsupported target " ~ target.type) }}
  {% endif %}
{% endmacro %}
