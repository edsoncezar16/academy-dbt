with
source_data as (
    select
        cast(stateprovinceid as int) as id_estado,
        cast(name as string) as estado,
        cast(countryregioncode as string) as codigo_pais
    from {{ source('indicium-ae-certification','stateprovince') }}
)

select *
from source_data