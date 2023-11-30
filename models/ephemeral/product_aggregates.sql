{{ 
    config(materialized='ephemeral')
}}

SELECT

    MD5(CAST(l_partkey AS STRING))        AS dim_product_key,
    l_partkey                             AS product_id,
    SUM(l_quantity)                       AS total_quantity_ordered,
    AVG(l_extendedprice)                  AS average_price,
    COUNT(DISTINCT l_orderkey)            AS total_orders

FROM {{source('silver_tpch','lineitem')}}
GROUP BY dim_product_key, l_partkey