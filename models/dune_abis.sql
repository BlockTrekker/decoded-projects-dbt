
{{ config(
    materialized='view',
    schema='decoded_contracts',
    name='decoded_contracts',
)
}}


SELECT 
  address.*,
  abi.abi
FROM 
  {{ source('decoded_projects','address_to_ordinal_dune') }}  address
LEFT JOIN
  {{ source('decoded_projects','abi_to_ordinal_dune') }} abi
ON
  abi.ordinal = address.ordinal