{{
    config(
        materialized='table'
    )
}}

SELECT
    DISTINCT
        customer_id,
        name
FROM {{ source('partna_transactions', 'transaction_data_wide_table') }} t
JOIN {{ ref('customer_map') }} c
ON t.customer_id = c.customer_id
WHERE customer_id IS NOT NULL