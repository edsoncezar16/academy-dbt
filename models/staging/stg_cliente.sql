with
source_data as (
    select
        cast(customerid as int) as id_cliente,
        cast(personid as int) as id_pessoa
    from {{ source('indicium-ae-certification','customer') }}
)

select *
from source_data