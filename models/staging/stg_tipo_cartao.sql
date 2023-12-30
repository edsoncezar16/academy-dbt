with
    source_data as (
        select
            distinct cardtype as  TIPO_CARTAO
        from {{source('indicium-ae-certification','creditcard')}}
)

select *
from source_data