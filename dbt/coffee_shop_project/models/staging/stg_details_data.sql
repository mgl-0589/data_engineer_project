{{ config(
    materialized='table',
    unique_key='id_details'
)}}

-- select * 
-- from {{ source('dev', 'raw_details_data') }}

with dedup_details as (
    select 
        *,
        ROW_NUMBER() OVER(PARTITION BY source, product_service_key ORDER BY date_insertion) AS rn 
    from {{ source('dev', 'raw_details_data') }}
)

select
    id_details,
    CAST(source AS VARCHAR(40)) AS source,
    CAST(product_service_key AS VARCHAR(50)) AS product_service_key,
    CAST(product_id AS VARCHAR(50)) AS product_id,
    CAST(quantity AS NUMERIC) AS quantity,
    CAST(unit_key AS VARCHAR(10)) AS unit_key,
    CAST(
        CASE
            WHEN LOWER(units) = 'bolsa' THEN 'bolsa'
            WHEN LOWER(units) IN ('pq', 'paquete', 'cajas') THEN 'paquete'
            WHEN LOWER(units) IN ('pza', 'pieza', 'piezas') THEN 'pieza'
            WHEN LOWER(units) IN ('serv', 'servicio', 'actividad', 'unidad de servicio', 'act') THEN 'servicio'
            WHEN LOWER(units) IN ('kilogramo', 'kilo', 'kg', 'kgm') THEN 'kilogramo'
            WHEN LOWER(units) IN ('litro', 'lt', 'ltr') THEN 'litro'
            WHEN LOWER(units) IN ('mt', 'mtr', 'metro') THEN 'metro'
            WHEN LOWER(units) = 'unidades' THEN 'unidades'
        END 
    AS VARCHAR(15)) AS units,
    description,
    CAST(unit_value AS NUMERIC) AS unit_value,
    CAST(amount AS NUMERIC) AS product_amount,
    CAST(discount AS NUMERIC) AS discount,
    CAST(amount_object AS VARCHAR(5)) AS amount_object,
    date_insertion
from dedup_details
where rn = 1