with
    source_data as (
        select
            cast(stateprovinceId as int) as ID_ESTADO
            , cast(name as string) as ESTADO
            , cast(countryregioncode as string) as CODIGO_PAIS
        from {{source('indicium-ae-certification','stateprovince')}}
)

select *
from source_data