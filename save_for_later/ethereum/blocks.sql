
{{ config(
    materialized='view',
    schema='ethereum',
    name='blocks',
)
}}


SELECT 
  *
FROM 
  {{ source('arakis','blocks') }}