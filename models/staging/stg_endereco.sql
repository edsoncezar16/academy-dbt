with
source_data as (
    select
        cast(addressid as int) as id_endereco,
        cast(city as string) as cidade,
        cast(stateprovinceid as int) as id_estado
    from {{ source('indicium-ae-certification','address') }}
)

select *
from source_data