with 
    staging as (
        select 
            stg_cliente.ID_CLIENTE,
            stg_pessoa.NOME_COMPLETO
        from {{ref('stg_cliente')}}
        LEFT JOIN {{ref('stg_pessoa')}}
        on stg_pessoa.ID_ENTIDADE_NEGOCIO = stg_cliente.ID_PESSOA
)
    , transformed as (
        select
            row_number() over (order by ID_CLIENTE) as SK_CLIENTE -- auto-incremental surrogate key
            , ID_CLIENTE
            , NOME_COMPLETO
        from staging
)

select *  from transformed