
{{ config(
    materialized='view',
    schema='ethereum',
    name='transactions',
)
}}


SELECT 
  *
FROM 
  {{ source('arakis','transactions') }}