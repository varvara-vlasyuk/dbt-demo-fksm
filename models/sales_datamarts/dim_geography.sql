{{
config(
    materialized='view'
    ,unique_key='dim_geography_key'
    ,tags='dim'
)
}}
SELECT
    MD5(CAST(n.n_nationkey AS STRING) ||
        CAST(n.n_regionkey AS STRING))      AS dim_geography_key,
    n.n_nationkey                           AS nation_key,
    n.n_name                                AS nation_name,
    r.r_regionkey                           AS region_key,
    r.r_name                                AS region_name
FROM {{source('silver_tpch','nation')}}     AS n
JOIN {{source('silver_tpch','region')}}     AS r ON n.n_regionkey = r.r_regionkey;