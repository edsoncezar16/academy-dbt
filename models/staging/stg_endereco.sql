with
    source_data as (
        select
            cast(addressid as int) as ID_ENDERECO
            , cast(city as string) as CIDADE
            , cast(stateprovinceid as int) as ID_ESTADO
        from {{source('indicium-ae-certification','address')}}
)

select *
from source_data