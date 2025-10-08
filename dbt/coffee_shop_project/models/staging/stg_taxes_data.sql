{{ config(
    materialized='table',
    unique_key='id_taxes'
)}}

select *
from {{ source('dev', 'raw_taxes_data') }}

-- with dedup_taxes as (
--     select 
--         *,
--         row_number() over(partition by source, base, amount order by date_insertion) as rn 
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
-- from dedup_taxes
-- where rn = 1