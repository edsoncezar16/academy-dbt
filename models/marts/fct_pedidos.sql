with cidade as (
   select
     SK_CIDADE,
     CIDADE
   FROM {{ref('dim_cidade')}}   
),

estado as (
   select
     SK_ESTADO
   , ID_ESTADO
   FROM {{ref('dim_estado')}}   
),

pais as (
    select
      SK_PAIS
      , CODIGO_PAIS
    FROM {{ref('dim_pais')}}
), 

cliente as (
   select
     SK_CLIENTE
     , ID_CLIENTE
   FROM {{ref('dim_cliente')}}   
),
 
data as (
   select
     DATA_VENDA
   FROM {{ref('dim_data_venda')}}   
),
 
produto as (
   select
     SK_PRODUTO
   , ID_PRODUTO
   FROM {{ref('dim_products')}}   
),

motivo as (
    select
     SK_MOTIVO_VENDA
     , ID_MOTIVO_VENDA
    FROM {{ref('dim_motivo_venda')}}
),

tipo_cartao as (
    select
        SK_TIPO_CARTAO,
        TIPO_CARTAO
    FROM {{ref('dim_tipo_cartao')}}
),

cabecalho_pedido as (
        select
            ID_VENDA
            , DATA_VENDA
            , ID_CLIENTE
            , ID_ENDERECO_FATURA
            , ID_CARTAO
        FROM {{ref('stg_cabecalho_pedido')}}
), 

detalhes_pedido as (
        select
            ID_VENDA
            , ID_PRODUTO
            , QUANTIDADE_COMPRADA
            , PRECO_UNITARIO
            , DESCONTO_UNITARIO
        FROM {{ref('stg_detalhamento_pedido')}}
),

cartao as (
    select
     *
    FROM {{ref('stg_cartao')}}
), 

endereco as (
    select
     *
    FROM {{ref('stg_endereco')}}
),

total_info as (
    select *
    from detalhes_pedido
    left join cabecalho_pedido on cabecalho_pedido.ID_VENDA = detalhes_pedido.ID_VENDA
    left join cartao on cabecalho_pedido.ID_CARTAO = cartao.ID_CARTAO
    left join endereco on cabecalho_pedido.ID_ENDERECO_FATURA = endereco.ID_ENDERECO
    left join dim_cidade on endereco.CIDADE = dim_cidade.CIDADE
)