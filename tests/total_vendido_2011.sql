-- If sum of gross sales in 2011 is not approximately (1%) $12.646.112,16, throws an error */
 
with
   vendas_2011 as (
       SELECT
           sum(VENDAS_BRUTAS) as total
       FROM {{ ref ('fct_linhas_pedidos') }}
       where extract(year from DATA_VENDA) = 2011
   )
 
select * from vendas_2011 where total not between 0.99 * 12646112.16 and 1.01 * 12646112.16