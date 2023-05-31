
{{ config(
    materialized='view',
    schema='ethereum',
    name='logs',
)
}}

SELECT 
  *
FROM 
  {{ source('arakis','logs') }}