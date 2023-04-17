{{ config(
    materialized='table',
    schema='decoded_contracts',
    name='decoded_contracts_json',
)
}}

WITH initial as (
  SELECT
    sub_name,
    address,
    TO_JSON_STRING(
      STRUCT(
        name,
        namespace,
        created_ts,
        hash_id,
        type,
        inputs,
        outputs
      )) AS data
  FROM (
    SELECT
      a.address,
      a.name,
      namespace,
      c.block_timestamp AS created_ts,
      CONCAT(namespace, "_", COALESCE(evt.name, call.name)) AS sub_name,
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
    sub_name,
    data,
    address
)

SELECT 
  sub_name,
  address as contract_address,
  TO_JSON_STRING(ARRAY_AGG(data)) as data
FROM initial
GROUP BY sub_name, address
ORDER BY sub_name
