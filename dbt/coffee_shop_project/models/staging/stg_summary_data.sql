{{ config(
    materialized='table',
    unique_key='id_generals'
)}}

select * from {{ source('dev', 'raw_summary_data') }}

-- with dedup_summary as (
--     select 
--         *,
--         row_number() over(partition by file_name, invoice_id order by date_insertion) as rn 
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
-- from dedup_summary
-- where rn = 1

