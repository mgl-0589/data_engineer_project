{{ config(
    materialized='table',
    unique_key='id_taxes'
)}}

select *
from {{ source('dev', 'raw_taxes_data') }}

-- with taxes_source as (
--     select 
--         *,
--         ROW_NUMBER() OVER(PARTITION BY source, product_service_key ORDER BY date_insertion) AS rn 
--     from {{ source('dev', 'raw_taxes_data') }}
-- )

-- select
--     id_taxes,
--     source,
--     base,
--     amount,
--     tax_id,
--     type_factor,
--     rate_or_share,
--     date_insertion
-- from taxes_source