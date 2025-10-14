{{ config(
    materialized='table',
    unique_key='id_sales'
)}}


with dedup_sales as (
    select 
        *,
        row_number() over(partition by sales_date) as rn 
    from {{ source('dev', 'raw_sales_data') }}
)

select
    id_sales,
    CAST(sales_date AS TIMESTAMP) AS sales_date,
    devolutions,
    sales_cash,
    sales_debit,
    sales_credit,
    sales_uber,
    sales_rappi,
    sales_transfer,
    num_tickets,
    avg_tickets,
    num_devolutions,
    total_devolutions,
    num_invitations,
    total_invitations,
    base_tax_16,
    amount_tax_16,
    base_tax_0,
    amount_tax_0,
    total_bases,
    total_taxes,
    total,
    total_cards,
    date_insertion
from dedup_sales
where rn = 1