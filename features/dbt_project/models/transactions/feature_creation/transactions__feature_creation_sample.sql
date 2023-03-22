--{# We set the table to run with incremental model to reduce the costs #}
{{ config(materialized='incremental')}}

SELECT *, 
FORMAT_DATE('%A', tx_ts)  AS day_of_the_week,
{{ 
    default__compute_rolling_windows(
    aggregations=["COUNT","AVG", "MIN"],
    columns_to_aggregate=["tx_amount"],
    partition="customer_id",
    timestamp_column="tx_ts",
    lookback_windows_start=["1h", "5d", "28d", "90d", "95d"],
    exclude_current_row=True
    )
}}
{{ 
    default__compute_rolling_windows(
    aggregations=["MAX"],
    columns_to_aggregate=["tx_amount"],
    partition="terminal_id",
    timestamp_column="tx_ts",
    lookback_windows_start=["1h", "3d", "28d", "90d", "175d"],
    exclude_current_row=True
    )
}}
CURRENT_TIMESTAMP() AS created_timestamp
FROM {{ ref('transactions__preprocessing') }}
WHERE True
{{ incremental_date_filter("tx_ts","tx_ts") }}