with
    source_data as (
        select
            productid as ID_PRODUTO
            , name as PRODUTO
        from {{source('indicium-ae-certification','product')}}
)

select *
from source_data