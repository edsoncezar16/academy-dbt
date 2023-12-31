with
    staging as (
        select
         *
        FROM {{ref('stg_pedidos_motivo_venda')}}
    )

select * from staging