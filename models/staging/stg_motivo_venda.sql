with
source_data as (
    select
        cast(salesreasonid as int) as id_motivo_venda,
        cast(name as string) as motivo_venda
    from {{ source('indicium-ae-certification','salesreason') }}
)

select *
from source_data