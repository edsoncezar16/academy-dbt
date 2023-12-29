with 
    staging as (
        select 
            ID_ESTADO,
            ESTADO
        from {{ref('stg_estado')}}
)
    , transformed as (
        select
            row_number() over (order by ID_ESTADO) as SK_ESTADO -- auto-incremental surrogate key
            , ID_ESTADO
            , ESTADO
        from staging
)

select *  from transformed