with 
    staging as (
        select 
            DATA_VENDA
            , MES
            , ANO
        from {{ref('stg_cabecalho_pedido')}}
)
    , transformed as (
        select
           *
        from staging
)

select *  from transformed