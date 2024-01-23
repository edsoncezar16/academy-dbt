with
    source_data as (
        select
            cast(productid as int) as ID_PRODUTO
            , cast(name as string) as PRODUTO
        from {{source('indicium-ae-certification','product')}}
)

select *
from source_data