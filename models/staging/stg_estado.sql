with
    source_data as (
        select
            stateprovinceId as ID_ESTADO
            , name as ESTADO
            , countryregioncode as CODIGO_PAIS
        from {{source('indicium-ae-certification','stateprovince')}}
)

select *
from source_data