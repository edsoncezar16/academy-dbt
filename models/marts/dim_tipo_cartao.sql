with 
    staging as (
        select 
            TIPO_CARTAO
        from {{ref('stg_tipo_cartao')}}
)
    , transformed as (
        select
            row_number() over (order by TIPO_CARTAO) as SK_TIPO_CARTAO -- auto-incremental surrogate key
            , TIPO_CARTAO
        from staging
)

select *  from transformed