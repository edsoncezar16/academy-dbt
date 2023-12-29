with 
    staging as (
        select 
            CIDADE
        from {{ref('stg_cidade')}}
)
    , transformed as (
        select
            row_number() over (order by CIDADE) as SK_CIDADE -- auto-incremental surrogate key
            , CIDADE
        from staging
)

select *  from transformed