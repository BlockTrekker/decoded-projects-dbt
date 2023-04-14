
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
  {{ ref('address_to_ordinal_dune') }}  address
LEFT JOIN
    {{ ref('abi_to_ordinal_dune') }} abi
ON
  abi.ordinal = address.ordinal