with
source_data as (
    select
        cast(salesorderid as int) as id_venda,
        cast(salesreasonid as int) as id_motivo_venda
    from {{ source('indicium-ae-certification', 'salesorderheadersalesreason') }}
)

select * from source_data