{{ config(
    materialized='table',
    schema='decoded_contracts',
    name='decoded_contracts_json',
)
}}

WITH ungrouped AS (
  SELECT
    sub_name,
    inputs,
    outputs,
    type,
    hash_id,
    MIN(created_ts) as min_created_ts,
    address,
    name as contract_name,
    namespace
  FROM (
    SELECT
      a.address,
      a.name,
      namespace,
      c.block_timestamp AS created_ts,
      CONCAT(namespace, '_', COALESCE(evt.name, call.name, '')) AS sub_name,
      CASE WHEN evt.name IS NOT NULL THEN 'event' ELSE 'call' END AS type,
      COALESCE(evt.hash_id, call.hash_id) AS hash_id,
      COALESCE(evt.inputs, call.inputs) AS inputs,
      call.outputs AS outputs,
    FROM
      {{ ref('dune_abis') }} a
    LEFT JOIN 
      {{ source('crypto_ethereum_contracts', 'contracts')}} c
    ON
      c.address = a.address
    LEFT JOIN UNNEST(blocktrekker.udfs.PARSE_ABI_EVENTS(abi,a.name)) AS evt
    ON
      evt.name IS NOT NULL
    LEFT JOIN UNNEST(blocktrekker.udfs.PARSE_ABI_FUNCTIONS(abi,a.name)) AS call
    ON
      call.name IS NOT NULL
  )
  GROUP BY
    sub_name, type, hash_id, contract_name, namespace, inputs, outputs, address
)

SELECT
  sub_name,
  inputs,
  outputs,
  type,
  hash_id,
  min_created_ts,
  TO_JSON_STRING(ARRAY_AGG(address)) as contract_addresses,
  contract_name,
  namespace
FROM ungrouped
GROUP BY sub_name, type, hash_id, contract_name, namespace, inputs, outputs, min_created_ts
  