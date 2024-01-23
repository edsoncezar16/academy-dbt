with
    source_data as (
        select
            cast(creditcardid as int) as ID_CARTAO
            , cast(cardtype as string) as  TIPO_CARTAO
        from {{source('indicium-ae-certification','creditcard')}}
)

select *
from source_data