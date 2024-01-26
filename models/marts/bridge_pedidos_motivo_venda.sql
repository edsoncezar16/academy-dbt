with
    staging as (
        select
         stg.ID_VENDA
         , dim.SK_MOTIVO_VENDA
        FROM {{ref('stg_pedidos_motivo_venda')}} stg
        LEFT JOIN {{ref('dim_motivo_venda')}} dim using (ID_MOTIVO_VENDA)
    )

select * from staging