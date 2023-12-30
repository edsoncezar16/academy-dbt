with
    source_data as (
        select
            salesorderid as ID_VENDA
            , product_id as ID_PRODUTO
            , orderqty as QUANTIDADE_COMPRADA
            , unitprice as PRECO_UNITARIO
            , unitpricediscount as DESCONTO_UNITARIO
        from {{source('indicium-ae-certification','salesorderdetail')}}
)

select 
    *
from source_data