with
    source_data as (
        select
            stateprovinceId as ID_ESTADO
            , name as ESTADO
        from {{source('indicium-ae-certification','stateprovince')}}
)

select *
from source_data