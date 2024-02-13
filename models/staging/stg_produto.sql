with
source_data as (
    select
        cast(productid as int) as id_produto,
        cast(name as string) as produto
    from {{ source('indicium-ae-certification','product') }}
)

select *
from source_data