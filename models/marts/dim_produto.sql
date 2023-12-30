with 
    staging as (
        select 
            ID_PRODUTO
            , PRODUTO
        from {{ref('stg_produto')}}
)
    , transformed as (
        select
            row_number() over (order by ID_PRODUTO) as SK_PRODUTO -- auto-incremental surrogate key
            , ID_PRODUTO
            , PRODUTO
        from staging
)

select *  from transformed