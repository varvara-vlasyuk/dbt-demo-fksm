{{
config(
    materialized='view'
    ,unique_key='dim_customer_key'
    ,tags='scd2'
)
}}

{{ scd2_actual(ref('dim_customer_base_scd2')) }}
