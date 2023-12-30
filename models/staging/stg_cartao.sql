with
    source_data as (
        select
            creditcardid as ID_CARTAO
            cardtype as  TIPO_CARTAO
        from {{source('indicium-ae-certification','creditcard')}}
)

select *
from source_data