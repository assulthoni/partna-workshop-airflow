{{
    config(
        materialized='view',
        view, table, mat+view, incremental
        schema='finance',
        tags=['schedule:7d']
    )
}}

SELECT
    DATE(purchase_date) "date",
    SUM(p.quantity * d.price) revenue,
    LAG(SUM(p.quantity * d.price)) OVER (ORDER BY DATE(purchase_date) ASC) lag_revenue
FROM {{ ref('fact_purchase') }} p
JOIN {{ ref('dim_product') }} d
ON p.product_id = d.product_id
GROUP BY "date"
