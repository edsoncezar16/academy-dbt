with 
    staging as (
        select 
            CODIGO_PAIS
            , PAIS
        from {{ref('stg_pais')}}
)
    , transformed as (
        select
            row_number() over (order by CODIGO_PAIS) as SK_PAIS -- auto-incremental surrogate key
            , CODIGO_PAIS
            , PAIS
        from staging
)

select *  from transformed