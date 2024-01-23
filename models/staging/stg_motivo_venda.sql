with
    source_data as (
        select
            cast(salesreasonId as int) as ID_MOTIVO_VENDA
            , cast(name as string) as MOTIVO_VENDA
        from {{source('indicium-ae-certification','salesreason')}}
)

select *
from source_data