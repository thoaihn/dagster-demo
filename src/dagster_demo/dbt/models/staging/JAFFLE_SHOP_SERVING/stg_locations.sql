{{
    config(
        materialized = 'table',
        cluster_by = ['location_id'],
        tags = ['daily']
    )
}}

with

source as (

    select * from {{ source('JAFFLE_SHOP_RAW', 'raw_stores') }}

),

renamed as (

    select

        ----------  ids
        id as location_id,

        ---------- text
        name as location_name,

        ---------- numerics
        tax_rate,

        ---------- timestamps
        {{ dbt.date_trunc('day', 'opened_at') }} as opened_date

    from source

)

select * from renamed
