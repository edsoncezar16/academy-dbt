with
    source_data as (
        select
            cast(salesorderid as int) as ID_VENDA
            , cast(productid as int) as ID_PRODUTO
            , cast(orderqty as int) as QUANTIDADE_COMPRADA
            , cast(unitprice as float64) as PRECO_UNITARIO
            , cast(unitpricediscount as float64) as DESCONTO_PERCENTUAL_UNITARIO
        from {{source('indicium-ae-certification','salesorderdetail')}}
)

select 
    *
from source_data