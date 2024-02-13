with
source_data as (
    select
        cast(salesorderdetailid as int) as id_detalhamento_pedido,
        cast(salesorderid as int) as id_venda,
        cast(productid as int) as id_produto,
        cast(orderqty as int) as quantidade_comprada,
        cast(unitprice as float64) as preco_unitario,
        cast(unitpricediscount as float64) as desconto_percentual_unitario
    from {{ source('indicium-ae-certification','salesorderdetail') }}
)

select *
from source_data