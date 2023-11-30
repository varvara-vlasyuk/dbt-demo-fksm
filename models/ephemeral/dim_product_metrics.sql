{{
config(
     unique_key='dim_product_key'
    ,tags='dim'
    ,alias='dim_product_agg'
)
}}

SELECT

    pa.dim_product_key             AS dim_product_key,
    pa.product_id                  AS product_id,
    MAX(pa.total_quantity_ordered) AS total_quantity_ordered,
    MAX(pa.average_price)          AS average_price,
    MAX(pa.total_orders)           AS total_orders,
    AVG(ps.ps_supplycost)          AS average_supply_cost,
    SUM(ps.ps_availqty)            AS total_available_quantity

FROM {{ ref('product_aggregates') }}           AS pa
LEFT JOIN {{source('silver_tpch','partsupp')}} AS ps ON pa.product_id = ps.ps_partkey
GROUP BY dim_product_key, pa.product_id
;
