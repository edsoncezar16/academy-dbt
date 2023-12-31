with 
    staging as (
        select
            ID_CARTAO
            , TIPO_CARTAO
        from {{ref('stg_cartao')}}
)
    , transformed as (
        select
            row_number() over (order by ID_CARTAO) as SK_CARTAO -- auto-incremental surrogate key
            , ID_CARTAO
            , TIPO_CARTAO
        from staging
)

select *  from transformed