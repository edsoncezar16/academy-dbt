with
source_data as (
    select
        cast(countryregioncode as string) as codigo_pais,
        cast(name as string) as pais
    from {{ source('indicium-ae-certification','countryregion') }}
)

select *
from source_data