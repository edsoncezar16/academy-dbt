with 
    staging as (
        select 
            DATA_VENDA
            , MES
            , ANO
        from {{ref('stg_data_venda')}}
)
    , transformed as (
        select
           *
        from staging
)

select *  from transformed