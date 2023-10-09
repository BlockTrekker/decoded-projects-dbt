
{{ config(
    materialized='table',
    schema='decoded_contracts',
    name='decoded_contracts',
    cluster_by=['name','namespace']
)
}}


SELECT 
  address.address,
  REPLACE(address.name, '.', '_') AS name,
  REPLACE(address.namespace, '.', '_') AS namespace,
  address.ordinal,
  abi.abi
FROM 
  {{ source('decoded_projects','address_to_ordinal_dune') }}  address
LEFT JOIN
  {{ source('decoded_projects','abi_to_ordinal_dune') }} abi
ON
  abi.ordinal = address.ordinal