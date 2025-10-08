{{ config(
    materialized='table',
    unique_key='id_details'
)}}

select * 
from {{ source('dev', 'raw_details_data') }}

-- with dedup_details as (
--     select 
--         *,
--         ROW_NUMBER() OVER(PARTITION BY source, product_service_key ORDER BY date_insertion) AS rn 
--     from {{ source('dev', 'raw_details_data') }}
-- )

-- select
--     id_details,
--     source,
--     product_service_key,
--     product_id,
--     quantity,
--     unit_key,
--     units,
--     description,
--     unit_value,
--     amount,
--     discount,
--     amount_object,
--     date_insertion
-- from dedup_details
-- where rn = 1