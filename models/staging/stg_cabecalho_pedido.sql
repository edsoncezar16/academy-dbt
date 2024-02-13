-- Configure the model as incremental
{{ config(materialized='incremental') }}

with
source_data as (
    select
        cast(salesorderid as int) as id_venda,
        cast(substr(orderdate, 1, 10) as date) as data_venda,
        cast(customerid as int) as id_cliente,
        cast(billtoaddressid as int) as id_endereco_fatura,
        cast(creditcardid as int) as id_cartao
    from {{ source('indicium-ae-certification','salesorderheader') }}
)

select *
from source_data
-- Use the Dagster partition variables to filter rows on an incremental run
{% if is_incremental() %}
    where data_venda >= '{{ var('min_date') }}' and data_venda <= '{{ var('max_date') }}'
{% endif %}