{{
    config(
        materialized = 'incremental',
        unique_key = 'customer_key',
        alias='dim_customer_base',
        tag='scd2'
    )
}}

{{ scd2_versioning(ref('stg_customer'), 'dim_customer_base', 'customer_key', ['load_date']) }}