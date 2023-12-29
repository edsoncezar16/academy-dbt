with
    source_data as (
        select
            countryregioncode as CODIGO_PAIS
            , name as PAIS
        from {{source('indicium-ae-certification','countryregion')}}
)

select *
from source_data