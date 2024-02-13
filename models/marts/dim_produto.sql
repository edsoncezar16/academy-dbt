with
staging as (
    select
        id_produto,
        produto
    from {{ ref('stg_produto') }}
),

transformed as (
    select
        id_produto,
        produto,
        row_number() over (order by id_produto) as sk_produto -- auto-incremental surrogate key
    from staging
)

select * from transformed