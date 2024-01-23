with 
    staging as (
        select
            ID_ENDERECO
            , CIDADE
            , ESTADO
            , PAIS 
        from {{ref('stg_endereco')}}
        left join {{ref('stg_estado')}} using (ID_ESTADO)
        left join {{ref('stg_pais')}} using (CODIGO_PAIS)
)
    , transformed as (
        select
            row_number() over (order by ID_ENDERECO) as SK_LOCALIDADE -- auto-incremental surrogate key
            , ID_ENDERECO AS ID_LOCALIDADE
            , CIDADE
            , ESTADO
            , PAIS
        from staging
)

select *  from transformed