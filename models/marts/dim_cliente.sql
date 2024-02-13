with
staging as (
    select
        stg_cliente.id_cliente,
        stg_pessoa.nome_completo
    from {{ ref('stg_cliente') }}
    left join {{ ref('stg_pessoa') }}
        on stg_cliente.id_pessoa = stg_pessoa.id_entidade_negocio
),

transformed as (
    select
        id_cliente,
        nome_completo,
        row_number() over (order by id_cliente) as sk_cliente -- auto-incremental surrogate key
    from staging
)

select * from transformed