with
staging as (
    select
        id_cartao,
        tipo_cartao
    from {{ ref('stg_cartao') }}
),

transformed as (
    select
        id_cartao,
        tipo_cartao,
        row_number() over (order by id_cartao) as sk_cartao -- auto-incremental surrogate key
    from staging
)

select * from transformed