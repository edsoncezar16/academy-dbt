with localidade as (
   select
     SK_LOCALIDADE,
     CIDADE
   FROM {{ref('dim_localidade')}}   
),

cliente as (
   select
     SK_CLIENTE
     , ID_CLIENTE
   FROM {{ref('dim_cliente')}}   
),
 
produto as (
   select
     SK_PRODUTO
   , ID_PRODUTO
   FROM {{ref('dim_produto')}}   
),

tipo_cartao as (
    select
        SK_TIPO_CARTAO
        , TIPO_CARTAO
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
            , DESCONTO_PERCENTUAL_UNITARIO
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

bridge_pedidos_motivo_venda as (
    select
      *
    FROM {{ref('stg_pedidos_motivo_venda')}}
),

motivo_venda as (
    select
      *
    FROM {{ref('dim_motivo_venda')}}
),

total_info as (
    select
        detalhes_pedido.ID_VENDA
        , detalhes_pedido.ID_PRODUTO
        , detalhes_pedido.QUANTIDADE_COMPRADA
        , detalhes_pedido.PRECO_UNITARIO
        , detalhes_pedido.DESCONTO_PERCENTUAL_UNITARIO
        , cabecalho_pedido.DATA_VENDA
        , localidade.SK_LOCALIDADE
        , cliente.SK_CLIENTE
        , produto.SK_PRODUTO
        , tipo_cartao.SK_TIPO_CARTAO
        , motivo_venda.SK_MOTIVO_VENDA
    FROM detalhes_pedido
    LEFT JOIN cabecalho_pedido on cabecalho_pedido.ID_VENDA = detalhes_pedido.ID_VENDA
    LEFT JOIN cartao on cabecalho_pedido.ID_CARTAO = cartao.ID_CARTAO
    LEFT JOIN tipo_cartao on cartao.TIPO_CARTAO = tipo_cartao.TIPO_CARTAO
    LEFT JOIN endereco on cabecalho_pedido.ID_ENDERECO_FATURA = endereco.ID_ENDERECO
    LEFT JOIN localidade on endereco.CIDADE = localidade.CIDADE
    LEFT JOIN cliente on cabecalho_pedido.ID_CLIENTE = cliente.ID_CLIENTE
    LEFT JOIN produto on detalhes_pedido.ID_PRODUTO = produto.ID_PRODUTO
    LEFT JOIN bridge_pedidos_motivo_venda on bridge_pedidos_motivo_venda.ID_VENDA = detalhes_pedido.ID_VENDA
    LEFT JOIN motivo_venda on motivo_venda.ID_MOTIVO_VENDA = bridge_pedidos_motivo_venda.ID_MOTIVO_VENDA
),

computa_metricas as (
    select
        SK_LOCALIDADE as LOCALIDADE_FK
        , SK_CLIENTE as CLIENTE_FK
        , SK_PRODUTO as PRODUTO_FK
        , SK_TIPO_CARTAO as TIPO_CARTAO_FK
        , DATA_VENDA
        , SK_MOTIVO_VENDA as MOTIVO_VENDA_FK
        , count( distinct ID_VENDA) as NUMERO_PEDIDOS
        , sum( QUANTIDADE_COMPRADA ) as QUANTIDADE_TOTAL_COMPRADA
        , sum( QUANTIDADE_COMPRADA * PRECO_UNITARIO ) as VALOR_TOTAL_BRUTO
        , sum( QUANTIDADE_COMPRADA * PRECO_UNITARIO * (1.0 - DESCONTO_PERCENTUAL_UNITARIO) ) as VALOR_TOTAL_LIQUIDO
    FROM total_info
    GROUP BY SK_LOCALIDADE, SK_CLIENTE, SK_PRODUTO, SK_TIPO_CARTAO, DATA_VENDA, SK_MOTIVO_VENDA
)

select * from computa_metricas