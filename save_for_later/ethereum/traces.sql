
{{ config(
    materialized='view',
    schema='ethereum',
    name='traces',
)
}}


SELECT 
  *
FROM 
  {{ source('arakis','traces') }}