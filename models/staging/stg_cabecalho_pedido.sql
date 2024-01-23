with
    source_data as (
        select
            cast(salesorderid as int) as ID_VENDA
            , cast(substr(orderdate, 1, 10) as date) as DATA_VENDA
            , cast(customerid as int) as ID_CLIENTE
            , cast(billtoaddressid as int) as ID_ENDERECO_FATURA
            , cast(creditcardid as int) as ID_CARTAO
        from {{source('indicium-ae-certification','salesorderheader')}}
)

select 
    *
from source_data