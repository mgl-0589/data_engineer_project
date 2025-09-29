{{ config(
    materialized='table',
    unique_key='id_generals'
)}}

select * from {{ source('dev', 'raw_summary_data') }}

-- with summary_source as (
--     select 
--         *,
--         ROW_NUMBER() OVER(PARTITION BY file_name, invoice_id ORDER BY date_insertion) AS rn 
--     from {{ source('dev', 'raw_summary_data') }}
-- )

-- select
--     id_stg_generals,
--     file_name,
--     date_creation,
--     invoice_id,
--     currency,
--     subtotal,
--     total,
--     transmitter_id,
--     transmitter_name,
--     transmitter_zip_code,
--     receiver_id,
--     receiver_name,
--     invoice_type,
--     store,
--     date_insertion
-- from summary_source
-- where rn = 1

