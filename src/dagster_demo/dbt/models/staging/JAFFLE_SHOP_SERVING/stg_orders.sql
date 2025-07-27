{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ordered_at",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ['customer_id', 'location_id'],
        tags = ['daily']
    )
}}

with

source as (

    select *
    from {{ source('JAFFLE_SHOP_RAW', 'raw_orders') }}
    where date(ordered_at, {{ var('dbt_date:time_zone') }}) >= date('{{ var('min_date') }}')
        and date(ordered_at, {{ var('dbt_date:time_zone') }}) < date('{{ var('max_date') }}')

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- numerics
        subtotal as subtotal_cents,
        tax_paid as tax_paid_cents,
        order_total as order_total_cents,
        {{ cents_to_dollars('subtotal') }} as subtotal,
        {{ cents_to_dollars('tax_paid') }} as tax_paid,
        {{ cents_to_dollars('order_total') }} as order_total,

        ---------- timestamps
        date(ordered_at, {{ var('dbt_date:time_zone') }}) as ordered_at

    from source

)

select * from renamed
