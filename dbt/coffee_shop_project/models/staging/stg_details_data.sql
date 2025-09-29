{{ config(
    materialized='table',
    unique_key='id_details'
)}}

select * 
from {{ source('dev', 'raw_details_data') }}

-- with details_source as (
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
-- from details_source
-- where rn = 1