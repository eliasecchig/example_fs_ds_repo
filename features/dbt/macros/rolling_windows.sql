
{%- macro default__rolling_window(aggregation, column_to_aggregate, partition, timestamp_column, frame_seconds_start, frame_seconds_end ) -%}
{{ aggregation }}({{ column_to_aggregate }}) OVER  (PARTITION BY {{ partition }} ORDER BY UNIX_SECONDS({{ timestamp_column }}) RANGE BETWEEN {{ frame_seconds_start }} PRECEDING AND {{ frame_seconds_end }})
{%- endmacro -%}

{%- macro cast_aggregation_start(aggregation) -%}
{%- if aggregation | lower != 'count'  -%}CAST({%- endif -%}
{%- endmacro -%}

{%- macro cast_aggregation_end(aggregation) -%}
{%- if aggregation | lower != 'count' %} AS FLOAT64){% endif -%}
{%- endmacro -%}

{%- macro default__compute_rolling_windows(aggregations, columns_to_aggregate, partition, timestamp_column, lookback_windows_start, exclude_current_row=True ) -%}
{% set seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800} -%}
{% set partition_columns = partition -%}
{% set partition_columns_name = partition.replace(',','_') | lower -%}
{% if exclude_current_row -%}
    {% set frame_seconds_end = "1 PRECEDING" -%}
    {% set lookback_window_end = "1s" -%}
{% else -%}
    {% set frame_seconds_end = "CURRENT ROW" -%}
    {% set lookback_window_end = "0s" -%}
{% endif -%}
{% for column_to_aggregate in columns_to_aggregate -%}
{% for aggregation in aggregations -%}
{% for lookback_window_start in lookback_windows_start -%}
{% set frame_seconds_start = (lookback_window_start[:-1] | int )  * (seconds_per_unit[lookback_window_start[-1]]) -%}    
{{- cast_aggregation_start(aggregation) -}}
{{- default__rolling_window(
aggregation=aggregation,
column_to_aggregate=column_to_aggregate,
partition=partition_columns,
timestamp_column=timestamp_column,
frame_seconds_start=frame_seconds_start,
frame_seconds_end=frame_seconds_end) }}{{ cast_aggregation_end(aggregation) }} AS {{ aggregation | lower }}_{{ column_to_aggregate | lower }}_by_{{ partition_columns_name }}_{{ lookback_window_start }}_{{ lookback_window_end }},
{% endfor -%}
{% endfor -%}
{% endfor -%}
{% endmacro -%}
