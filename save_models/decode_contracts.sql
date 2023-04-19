{{ config(
    materialized='table',
    schema='decoded_contracts',
    name='decoded_contracts',
)
}}

WITH ungrouped AS (
  SELECT
    sub_name,
    inputs,
    outputs,
    type,
    hash_id,
    created_ts as min_created_ts,
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
      CASE WHEN evt.name IS NOT NULL THEN evt.hash_id ELSE call.hash_id END as hash_id,
      CASE WHEN evt.name IS NOT NULL THEN evt.inputs ELSE call.inputs END as inputs,
      CASE WHEN evt.name IS NOT NULL THEN NULL ELSE call.outputs END AS outputs
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
    sub_name, type, hash_id, contract_name, namespace, inputs, outputs, address, min_created_ts
)

SELECT
  sub_name,
  type,
  min(min_created_ts) as min_created_ts,
  TO_JSON_STRING(ARRAY_AGG(DISTINCT inputs)) as inputs,
  TO_JSON_STRING(ARRAY_AGG(DISTINCT outputs)) as outputs,
  TO_JSON_STRING(ARRAY_AGG(DISTINCT hash_id)) as hash_ids,
  TO_JSON_STRING(ARRAY_AGG(DISTINCT address)) as contract_addresses,
  contract_name,
  namespace
FROM ungrouped
GROUP BY sub_name, type, contract_name, namespace
  