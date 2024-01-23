with
    source_data as (
        select
            cast(countryregioncode as string) as CODIGO_PAIS
            , cast(name as string) as PAIS
        from {{source('indicium-ae-certification','countryregion')}}
)

select *
from source_data