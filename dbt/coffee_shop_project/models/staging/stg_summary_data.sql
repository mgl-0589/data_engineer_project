{{ config(
    materialized='table',
    unique_key='id_generals'
)}}

-- select * from {{ source('dev', 'raw_summary_data') }}

with dedup_summary as (
    select 
        *,
        row_number() over(partition by file_name, invoice_id order by date_insertion) as rn 
    from {{ source('dev', 'raw_summary_data') }}
)

select
    id_generals,
    CAST(file_name AS VARCHAR(40)) AS file_name,
    CAST(date_creation AS TIMESTAMP) AS cate_creation,
    CAST(invoice_id AS VARCHAR(10)) AS invoice_id,
    CAST(currency AS VARCHAR(5)) AS currency,
    CAST(subtotal AS NUMERIC) AS subtotal,
    CAST(total AS NUMERIC) AS total,
    CAST(transmitter_id AS VARCHAR(15)) AS transmitter_id,
    CAST(transmitter_name AS VARCHAR(100)) AS transmitter_name,
    CAST(transmitter_zip_code AS VARCHAR(15)) AS transmitter_zip_code,
    CAST(receiver_id AS VARCHAR(15)) AS receiver_id,
    CAST(receiver_name AS VARCHAR(100)) AS receiver_name,
    CAST(invoice_type AS VARCHAR(1)) AS invoice_type,
    CAST(store AS VARCHAR(3)) AS store,
    date_insertion
from dedup_summary
where rn = 1

