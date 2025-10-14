{{ config(
    materialized='table',
    unique_key='id_taxes'
)}}

-- select *
-- from {{ source('dev', 'raw_taxes_data') }}

with dedup_taxes as (
    select 
        *,
        row_number() over(partition by source, base, amount order by date_insertion) as rn 
    from {{ source('dev', 'raw_taxes_data') }}
)

select
    id_taxes,
    CAST(source AS VARCHAR(40)) AS source,
    CAST(base AS NUMERIC) AS tax_base,
    CAST(amount AS NUMERIC) AS tax_amount,
    CAST(tax_id AS VARCHAR(5)) AS tax_id,
    CAST(type_factor AS VARCHAR(10)) AS type_factor,
    CAST(rate_or_share AS NUMERIC) AS rate_or_share,
    date_insertion
from dedup_taxes
where rn = 1