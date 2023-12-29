with
    source_data as (
        select
            salesreasonId as ID_MOTIVO_VENDA
            , name as MOTIVO_VENDA
        from {{source('indicium-ae-certification','salesreason')}}
)

select *
from source_data