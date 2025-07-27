{{
    config(
        materialized = 'table',
        cluster_by = ['order_id'],
        tags = ['daily']
    )
}}

with

source as (

    select * from {{ source('JAFFLE_SHOP_RAW', 'raw_items') }}

),

renamed as (

    select

        ----------  ids
        id as order_item_id,
        order_id,
        sku as product_id

    from source

)

select * from renamed
