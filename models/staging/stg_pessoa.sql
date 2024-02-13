with
source_data as (
    select
        cast(businessentityid as int) as id_entidade_negocio,
        concat(
            coalesce(title, ''), ' ',
            coalesce(firstname, ''), ' ',
            coalesce(lastname, ''), ' ',
            coalesce(suffix, '')
        ) as nome_completo
    from {{ source('indicium-ae-certification','person') }}
)

select *
from source_data