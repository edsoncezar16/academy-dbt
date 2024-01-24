with 
    staging as (
        select 
            distinct DATA_VENDA
        from {{ref('stg_cabecalho_pedido')}}
)
    , transformed as (
        select
           *
           , extract(month from DATA_VENDA) as MES
           , extract(year from DATA_VENDA) as ANO
        from staging
)

select *  from transformed