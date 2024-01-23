with
    source_data as (
        select
            cast(customerId as int) as ID_CLIENTE
            , cast(personId as int) as ID_PESSOA
        from {{source('indicium-ae-certification','customer')}}
)

select *
from source_data