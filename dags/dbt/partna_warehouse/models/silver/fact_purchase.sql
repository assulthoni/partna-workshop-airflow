{{
    config(
        materialized='table',
        tags=['schedule:1d']
    )
}}

SELECT
    product_id,
    customer_id,
    purchase_date,
    1 AS quantity
FROM {{ source('partna_transactions', 'transaction_data_wide_table') }}
WHERE product_id IS NOT NULL
AND customer_id IS NOT NULL