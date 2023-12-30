with
    source_data as (
        select
            addressid as ID_ENDERECO
            , city as CIDADE
            , stateprovinceid as ID_ESTADO
        from {{source('indicium-ae-certification','address')}}
)

select *
from source_data