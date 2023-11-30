SELECT 1
FROM {{ref('fact_sales')}}
WHERE order_date <= ship_date