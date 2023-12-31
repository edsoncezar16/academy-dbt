with
    source_data as (
        select
            salesorderid as ID_VENDA
            , productid as ID_PRODUTO
            , orderqty as QUANTIDADE_COMPRADA
            , unitprice as PRECO_UNITARIO
            , unitpricediscount as DESCONTO_PERCENTUAL_UNITARIO
        from {{source('indicium-ae-certification','salesorderdetail')}}
)

select 
    *
from source_data