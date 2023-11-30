{{
config(
    unique_key='dim_customer_key'
    ,tags='dim'
)
}}
SELECT
    MD5(CAST(c.c_custkey AS STRING)) AS dim_customer_key,
    c.c_custkey AS customer_key,
    c.c_name AS name,
    c.c_address AS address,
    {{ remove_dashes('c.c_phone') }} AS phone,
    c.c_acctbal AS account_balance,
    c.c_mktsegment AS market_segment,
    MD5(CAST(c.c_nationkey AS STRING) || CAST(n.n_regionkey AS STRING)) AS dim_geography_key
FROM {{source('silver_tpch','customer')}}     AS c
JOIN {{source('silver_tpch','nation')}}       AS n ON c.c_nationkey = n.n_nationkey
JOIN {{source('silver_tpch','region')}}       AS r ON n.n_regionkey = r.r_regionkey;