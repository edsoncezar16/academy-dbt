with 
    staging as (
        select 
            distinct DATA_VENDA
        from {{ref('stg_cabecalho_pedido')}}
)
    , transformed as (
        select
           *
           , format_date('%m', DATA_VENDA) as MES
           , format_date('%Y', DATA_VENDA) as ANO
        from staging
)

select *  from transformed