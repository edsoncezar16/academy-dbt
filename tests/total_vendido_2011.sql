-- If sum of gross sales in 2011 is not approximately $12.646.112,16, throws an error */
 
with
   vendas_2011 as (
       SELECT
           sum(VALOR_TOTAL_BRUTO) as total
       FROM {{ ref ('fct_pedidos') }}
       LEFT JOIN {{ref('dim_data_venda')}} ON dim_data_venda.DATA_VENDA = fct_pedidos.DATA_VENDA
       where ANO = '2011'
   )
 
select * from vendas_2011 where total not between 12646112.1 and 12646112.2