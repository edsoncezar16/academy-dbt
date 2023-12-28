with
    source_data as (
        select
            businessentityId as ID_ENTIDADE_NEGOCIO
            , concat(firstname, ' ', middlename, ' ', lastname) as NOME_COMPLETO
        from {{source('indicium-ae-certification','person')}}
)

select *
from source_data