with
staging as (
    select
        id_motivo_venda,
        motivo_venda
    from {{ ref('stg_motivo_venda') }}
),

transformed as (
    select
        id_motivo_venda,
        motivo_venda,
        row_number() over (order by id_motivo_venda) as sk_motivo_venda -- auto-incremental surrogate key
    from staging
)

select * from transformed