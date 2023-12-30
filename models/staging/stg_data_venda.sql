with
    source_data as (
        select
            cast(substr(orderdate, 1, 10) as date) as DATA_VENDA
        from {{source('indicium-ae-certification','salesorderheader')}}
)

select 
    DATA_VENDA
    , format_date('%m', DATA_VENDA) as MES
    , format_date('%Y', DATA_VENDA) as ANO
from source_data