with
    source_data as (
        select
            salesorderid as ID_VENDA
            , cast(substr(orderdate, 1, 10) as date) as DATA_VENDA
            , customerid as ID_CLIENTE
            , billtoaddressid as ID_ENDERECO_FATURA
            , creditcardid as ID_CARTAO
        from {{source('indicium-ae-certification','salesorderheader')}}
)

select 
    *
from source_data