{{
    config(
        materialized='table'
    )
}}

SELECT
    DISTINCT
        product_id,
        category,
        price,
        ratings
FROM {{ source('partna_transactions', 'transaction_data_wide_table') }}
WHERE product_id IS NOT NULL