with 
    staging as (
        select 
            stg_cliente.ID_CLIENTE,
            stg_pessoa.NOME_COMPLETO
        from {{ref('stg_cliente')}}
        join {{ref('stg_contatoentidagenegocio')}}
        on stg_cliente.ID_PESSOA = stg_contatoentidagenegocio.ID_PESSOA
        join {{ref('stg_pessoa')}}
        on stg_contatoentidagenegocio.ID_ENTIDADE_NEGOCIO = stg_pessoa.ID_ENTIDADE_NEGOCIO
)
    , transformed as (
        select
            row_number() over (order by ID_CLIENTE) as SK_CLIENTE -- auto-incremental surrogate key
            , ID_CLIENTE
            , NOME_COMPLETO
        from staging
)

select *  from transformed