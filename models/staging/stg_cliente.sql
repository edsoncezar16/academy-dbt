with
    source_data as (
        select
            customerId as ID_CLIENTE
            , personId as ID_PESSOA
        from {{source('indicium-ae-certification','customer')}}
)

select *
from source_data