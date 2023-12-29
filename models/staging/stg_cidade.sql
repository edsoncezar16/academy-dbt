with
    source_data as (
        select
            distinct city as CIDADE
        from {{source('indicium-ae-certification','address')}}
)

select *
from source_data