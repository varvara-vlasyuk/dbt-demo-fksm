{{
    config(
        unique_key='dim_supplier_key'
        ,tags='dim'
    )
}}

SELECT
    MD5(CAST(s.s_suppkey AS STRING)) AS dim_supplier_key,
    s.s_suppkey AS supplier_key,
    s.s_name AS name,
    s.s_address AS address,
    {{ remove_dashes('s.s_phone') }}  AS phone,
    s.s_acctbal AS account_balance,
    MD5(CAST(s.s_nationkey AS STRING) || CAST(n.n_regionkey AS STRING)) AS dim_geography_key
FROM {{source('silver_tpch','supplier')}}     AS s
JOIN {{source('silver_tpch','nation')}}       AS n ON s.s_nationkey = n.n_nationkey
JOIN {{source('silver_tpch','region')}}       AS r ON n.n_regionkey = r.r_regionkey;