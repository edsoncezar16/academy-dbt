with
staging as (
    select
        endereco.id_endereco,
        endereco.cidade,
        endereco.estado,
        pais.pais
    from {{ ref('stg_endereco') }} as endereco
    left join {{ ref('stg_estado') }} as estado on endereco.id_estado = estado.id_estado
    left join {{ ref('stg_pais') }} as pais on endereco.codigo_pais = pais.codigo_pais
),

transformed as (
    select
        id_endereco as id_localidade,
        cidade,
        estado,
        pais,
        row_number() over (order by id_endereco) as sk_localidade -- auto-incremental surrogate key
    from staging
)

select * from transformed