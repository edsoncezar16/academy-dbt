with 
    staging as (
        select 
            ID_MOTIVO_VENDA
            , MOTIVO_VENDA
        from {{ref('stg_motivo_venda')}}
)
    , transformed as (
        select
            row_number() over (order by ID_MOTIVO_VENDA) as SK_MOTIVO_VENDA -- auto-incremental surrogate key
            , ID_MOTIVO_VENDA
            MOTIVO_VENDA
        from staging
)

select *  from transformed