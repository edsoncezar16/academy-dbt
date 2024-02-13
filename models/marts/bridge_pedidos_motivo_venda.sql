with
staging as (
    select
        stg.id_venda,
        dim.sk_motivo_venda
    from {{ ref('stg_pedidos_motivo_venda') }} as stg
    left join {{ ref('dim_motivo_venda') }} as dim on stg.id_motivo_venda = dim.id_motivo_venda
)

select * from staging