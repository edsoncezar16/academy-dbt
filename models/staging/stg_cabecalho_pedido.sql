with
    source_data as (
        select
            salesorderid as ID_VENDA
            , cast(substr(orderdate, 1, 10) as date) as DATA_VENDA
            , customer_id as ID_CLIENTE
            , billtoaddressid as ID_ENDERECO_FATURA
            , creditcardid as ID_CARTAO
        from {{source('indicium-ae-certification','salesorderheader')}}
)

select 
    *
    , format_date('%m', DATA_VENDA) as MES
    , format_date('%Y', DATA_VENDA) as ANO
from source_data