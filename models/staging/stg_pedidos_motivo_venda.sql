with
    source_data as (
        select
          cast(salesorderid as int) as ID_VENDA
          , cast(salesreasonid as int) as ID_MOTIVO_VENDA
        FROM {{source('indicium-ae-certification', 'salesorderheadersalesreason')}}
    )

select * from source_data