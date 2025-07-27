{{
    config(
        materialized = 'table',
        cluster_by = ['customer_id'],
        tags = ['daily']
    )
}}

with

source as (

    select * from {{ source('JAFFLE_SHOP_RAW', 'raw_customers') }}

),

renamed as (

    select

        ----------  ids
        id as customer_id,

        ---------- text
        name as customer_name

    from source

)

select * from renamed
