with
    source_data as (
        select
            cast(businessentityId as int) as ID_ENTIDADE_NEGOCIO
            , concat(
                coalesce(title, ''), ' '
                , coalesce(firstname, ''), ' '
                , coalesce(lastname, ''), ' '
                , coalesce(suffix, '')
                ) as NOME_COMPLETO
        from {{source('indicium-ae-certification','person')}}
)

select *
from source_data