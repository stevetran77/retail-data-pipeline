{% macro parse_dirty_timestamp(expression) %}
  coalesce(
    safe_parse_timestamp('%Y-%m-%dT%H:%M:%S', cast({{ expression }} as string)),
    safe_parse_timestamp('%Y-%m-%d %H:%M:%S', cast({{ expression }} as string)),
    safe_parse_timestamp('%m-%d-%Y %H:%M:%S', cast({{ expression }} as string)),
    safe_parse_timestamp('%m/%d/%Y %H:%M:%S', cast({{ expression }} as string)),
    safe_parse_timestamp('%Y/%m/%d %H:%M:%S', cast({{ expression }} as string))
  )
{% endmacro %}
