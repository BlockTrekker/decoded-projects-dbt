
{{ config(materialized='table') }}


WITH cl_prices as (
  SELECT 
    c.token_name,
    DATE_TRUNC(block_timestamp, minute) as minute,
    CAST(udfs.hexToInt(topics[SAFE_OFFSET(1)]) AS FLOAT64)/1e8 as price
  FROM `blocktrekker.contract_tables.clustered_logs` l
  LEFT JOIN (
    SELECT
      LOWER(contractAddress) as contractAddress,
      assetName as token_name
    FROM `blocktrekker.decoded_projects.chainlink_mapping`
    WHERE second_key = 'USD') c
    ON c.contractAddress = l.address
  WHERE evt_id = '0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f'
  AND c.token_name IS NOT NULL
  ORDER BY block_timestamp
),

minute_array as (
  SELECT
    TIMESTAMP_ADD('2021-05-1', INTERVAL minute_offset MINUTE) as minute
  FROM
    UNNEST(GENERATE_ARRAY(0,TIMESTAMP_DIFF(CURRENT_TIMESTAMP,TIMESTAMP('2021-05-1'),minute))) as minute_offset
  -- WHERE TIMESTAMP_ADD('2023-03-22', INTERVAL minute_offset MINUTE) < CURRENT_TIMESTAMP()
  ORDER BY minute
)

SELECT
 COALESCE(cl.token_name, LAST_VALUE(cl.token_name IGNORE NULLS) OVER (ORDER BY m.minute ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) as token_names,
 COALESCE(cl.price, LAST_VALUE(cl.price IGNORE NULLS) OVER (ORDER BY m.minute ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) as price,
 m.minute,
FROM minute_array m
LEFT JOIN cl_prices cl
  ON cl.minute = m.minute
ORDER BY m.minute