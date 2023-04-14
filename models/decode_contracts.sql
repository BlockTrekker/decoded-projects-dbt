{{ config(
    materialized='table',
    schema='decoded_contracts',
    name='decoded_contracts_json',
)
}}

WITH initial as (
  SELECT
    sub_name,
    TO_JSON_STRING(
      STRUCT(
        address,
        name,
        namespace,
        created_ts,
        signature,
        type
      )
    ) AS data
  FROM (
    SELECT
      a.address,
      a.name,
      namespace,
      c.block_timestamp AS created_ts,
      COALESCE(evt.name, call.name) AS sub_name,
      COALESCE(evt.signature, call.signature) AS signature,
      CASE WHEN evt.name IS NOT NULL THEN 'event' ELSE 'call' END AS type
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
    data
)

SELECT 
  sub_name,
  TO_JSON_STRING(ARRAY_AGG(data)) as data
FROM initial
GROUP BY sub_name
ORDER BY sub_name