with
    source_data as (
        select
            personId as ID_PESSOA
            , businessentityId as ID_ENTIDADE_NEGOCIO
        from {{source('indicium-ae-certification','businessentitycontact')}}
)

select *
from source_data