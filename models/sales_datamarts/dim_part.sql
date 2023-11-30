{{
config(
     unique_key='dim_part_key'
    ,tags='dim'
)
}}

SELECT

    MD5(CAST(p.p_partkey AS STRING) || 
        CAST(ps.ps_suppkey AS STRING)) AS dim_part_key,
    p.p_partkey                        AS part_key,
    p.p_name                           AS name,
    p.p_mfgr                           AS manufacturer,
    p.p_brand                          AS brand,
    p.p_type                           AS type,
    p.p_size                           AS size,
    p.p_container                      AS container,
    p.p_retailprice                    AS retail_price,
    ps.ps_availqty                     AS available_quantity,
    ps.ps_supplycost                   AS supply_cost,
    ps.ps_suppkey                      AS supplier_key,
    MD5(CAST(ps.ps_suppkey AS STRING)) AS dim_supplier_key

FROM {{source('silver_tpch','part')}}     AS p
JOIN {{source('silver_tpch','partsupp')}} AS ps ON p.p_partkey = ps.ps_partkey;