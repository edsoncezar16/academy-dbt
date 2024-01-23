with
    staging as (
        select
         ID_VENDA
         , SK_MOTIVO_VENDA
        FROM {{ref('stg_pedidos_motivo_venda')}}
        LEFT JOIN {{ref('dim_motivo_venda')}} using ID_MOTIVO_VENDA
    )

select * from staging