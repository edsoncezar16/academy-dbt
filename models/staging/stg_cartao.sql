with
source_data as (
    select
        cast(creditcardid as int) as id_cartao,
        cast(cardtype as string) as tipo_cartao
    from {{ source('indicium-ae-certification','creditcard') }}
)

select *
from source_data