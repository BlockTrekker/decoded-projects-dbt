

SELECT
  a.address,
  name,
  namespace,
  c.block_timestamp as created_ts,
  TO_JSON_STRING(blocktrekker.udfs.PARSE_ABI_EVENTS(abi,name)) as evts,
  TO_JSON_STRING(blocktrekker.udfs.PARSE_ABI_FUNCTIONS(abi,name)) as calls,
FROM
  `blocktrekker.decoded_projects.dune_abis` a
LEFT JOIN 
  `bigquery-public-data.crypto_ethereum.contracts` c
ON
  c.address = a.address