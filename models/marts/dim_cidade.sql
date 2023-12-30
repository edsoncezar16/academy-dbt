with 
    staging as (
        select 
            distinct CIDADE
        from {{ref('stg_endereco')}}
)
    , transformed as (
        select
            row_number() over (order by CIDADE) as SK_CIDADE -- auto-incremental surrogate key
            , CIDADE
        from staging
)

select *  from transformed