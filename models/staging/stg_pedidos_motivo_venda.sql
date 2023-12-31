with
    source_data as (
        select
          salesorderid as ID_VENDA
          , salesreasonid as ID_MOTIVO_VENDA
        FROM {{source('indicium-ae-certification', 'salesorderheadersalesreason')}}
    )

select * from source_data