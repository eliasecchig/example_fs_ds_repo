-- Because this data comes from a public dataset, we materialise the table here. Otherwise for this step the creation
-- of a view can be considered

-- query to join labels with features -------------------------------------------------------------------------------------------
{{ config(materialized='view') }}

SELECT
raw_tx.tx_ts,
raw_tx.tx_id,
raw_tx.customer_id,
raw_tx.terminal_id,
raw_tx.tx_amount,
raw_lb.tx_fraud
FROM {{ source('transactions', 'tx') }} AS raw_tx
LEFT JOIN {{ source('transactions', 'txlabels') }} AS raw_lb
ON raw_tx.TX_ID = raw_lb.TX_ID

--WHERE TRUE AND {{ incremental_date_filter("raw_tx.TX_TS","TX_TS") }}
