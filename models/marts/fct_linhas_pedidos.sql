with localidade as (
   select
     SK_LOCALIDADE,
     ID_LOCALIDADE
   FROM {{ref('dim_localidade')}}   
),

cliente as (
   select
     SK_CLIENTE
     , ID_CLIENTE
   FROM {{ref('dim_cliente')}}   
),
 
cartao as (
    select
        SK_CARTAO
        , ID_CARTAO
    FROM {{ref('dim_cartao')}}
), 

data as (
    select
        DATA_VENDA
    FROM {{ref('dim_data_venda')}}
), 


cabecalho_pedido_com_sk as (
        select
            ID_VENDA                        
            , localidade.SK_LOCALIDADE as FK_LOCALIDADE
            , cliente.SK_CLIENTE as FK_CLIENTE
            , cartao.SK_CARTAO as FK_CARTAO
            , data.DATA_VENDA
        FROM {{ref('stg_cabecalho_pedido')}} scp
        LEFT JOIN localidade on localidade.ID_LOCALIDADE = scp.ID_ENDERECO_FATURA
        LEFT JOIN cliente using (ID_CLIENTE)
        LEFT JOIN cartao using (ID_CARTAO)
        LEFT JOIN data using (DATA_VENDA)
), 

produto as (
   select
     SK_PRODUTO
   , ID_PRODUTO
   FROM {{ref('dim_produto')}}   
),


detalhes_pedido_com_sk as (
        select
            ID_DETALHAMENTO_PEDIDO
            , ID_VENDA
            , produto.SK_PRODUTO as FK_PRODUTO
            , QUANTIDADE_COMPRADA
            , PRECO_UNITARIO
            , DESCONTO_PERCENTUAL_UNITARIO
        FROM {{ref('stg_detalhamento_pedido')}} sdp
        LEFT JOIN produto using (ID_PRODUTO)
),

bridge as (
    select 
        ID_VENDA
        , SK_MOTIVO_VENDA
    FROM {{ref('bridge_pedidos_motivo_venda')}}
),

motivo as (
    select
        SK_MOTIVO_VENDA
    FROM {{ref('dim_motivo_venda')}}
), 

final as (
    select
        detalhes_pedido_com_sk.ID_VENDA
        , DATA_VENDA
        , FK_PRODUTO
        , FK_LOCALIDADE
        , FK_CLIENTE
        , FK_CARTAO
        , motivo.SK_MOTIVO_VENDA as FK_MOTIVO_VENDA
        , QUANTIDADE_COMPRADA / count(*) over (partition by ID_DETALHAMENTO_PEDIDO) as QUANTIDADE_COMPRADA_ALOCADA_POR_MOTIVO
        , PRECO_UNITARIO * QUANTIDADE_COMPRADA / count(*) over (partition by ID_DETALHAMENTO_PEDIDO) as VENDAS_BRUTAS_ALOCADAS_POR_MOTIVO
        , PRECO_UNITARIO * QUANTIDADE_COMPRADA * (1.0 - DESCONTO_PERCENTUAL_UNITARIO) / count(*) over (partition by ID_DETALHAMENTO_PEDIDO) as VENDAS_LIQUIDAS_ALOCADAS_POR_MOTIVO
    FROM detalhes_pedido_com_sk
    LEFT JOIN cabecalho_pedido_com_sk using (ID_VENDA)
    LEFT JOIN bridge using (ID_VENDA)
    LEFT JOIN motivo using (SK_MOTIVO_VENDA)
)

select * from final