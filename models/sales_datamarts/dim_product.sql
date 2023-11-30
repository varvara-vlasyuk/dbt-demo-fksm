{{
config(
     unique_key='dim_product_key'
    ,tags='dim'
)
}}
SELECT
    MD5(CAST(p_partkey AS STRING)) AS dim_product_key,
    p_name AS product_name,
    p_mfgr AS manufacturer,
    p_brand AS brand,
    p_type AS product_type,
    p_size AS size,
    p_container AS container,
    p_retailprice AS retail_price
FROM {{source('silver_tpch','part')}}
;