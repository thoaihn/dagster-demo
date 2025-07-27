{{
    config(
        materialized = 'table',
        cluster_by = ['supply_id', 'product_id'],
        tags = ['daily']
    )
}}

with

source as (

    select * from {{ source('JAFFLE_SHOP_RAW', 'raw_supplies') }}

),

renamed as (

    select

        ----------  ids
        {{ dbt_utils.generate_surrogate_key(['id', 'sku']) }} as supply_uuid,
        id as supply_id,
        sku as product_id,

        ---------- text
        name as supply_name,

        ---------- numerics
        {{ cents_to_dollars('cost') }} as supply_cost,

        ---------- booleans
        perishable as is_perishable_supply

    from source

)

select * from renamed
