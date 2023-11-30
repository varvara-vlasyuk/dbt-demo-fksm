{{
config(
    unique_key='fact_sales_key'
    ,tags='fact'
    ,alias='fact_sales'
)
}}

SELECT
    MD5(CAST(o.o_orderkey AS STRING) ||
        CAST(l.l_partkey AS STRING) ||
        CAST(l.l_suppkey AS STRING))            AS fact_sales_key,
    o.o_orderkey                                AS order_key,
    o.o_custkey                                 AS customer_key,
    l.l_partkey                                 AS part_key,
    l.l_suppkey                                 AS supplier_key,
    o.o_totalprice                              AS total_price,
    l.l_extendedprice                           AS extended_price,
    l.l_discount                                AS discount,
    l.l_quantity                                AS quantity,
    l.l_tax                                     AS tax,
    o.o_orderdate                               AS order_date,
    l.l_shipdate                                AS ship_date,
    l.l_commitdate                              AS commit_date,
    l.l_receiptdate                             AS receipt_date,
    cust.dim_customer_key                       AS dim_customer_key,
    part.dim_part_key                           AS dim_part_key,
    supp.dim_supplier_key                       AS dim_supplier_key
FROM {{source('silver_tpch','orders')}} AS o
JOIN {{source('silver_tpch','lineitem')}} AS l ON o.o_orderkey = l.l_orderkey
LEFT JOIN {{ref('dim_customer')}} AS cust    ON cust.dim_customer_key = MD5(CAST(o.o_custkey AS STRING))
LEFT JOIN {{ref('dim_supplier')}} AS supp    ON supp.dim_supplier_key = MD5(CAST(l.l_suppkey AS STRING))
LEFT JOIN {{ref('dim_part')}} AS part        ON part.dim_part_key = MD5(CAST(l.l_partkey AS STRING) || CAST(l.l_suppkey AS STRING))
;