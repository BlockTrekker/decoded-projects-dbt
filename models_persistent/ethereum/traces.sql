
{{ config(
    materialized='incremental',
    PartitionBy='TIMESTAMP_TRUNC(block_time, DAY)',
    name = 'traces',
    schema = 'ethereum'
)}}

SELECT
tr.block_timestamp AS block_time,
tr.block_number,
tr.value,
tr.gas,
tr.gas_used,
tr.block_hash,
CASE 
  WHEN status = 1 THEN TRUE
  ELSE FALSE END AS success, 
tr.transaction_index AS tx_index,
tr.subtraces AS sub_traces,
tr.`error`,
CASE 
  WHEN tx.receipt_status = 1 THEN TRUE
  ELSE FALSE END AS tx_success, --TODO requires aggregation
tr.transaction_hash AS tx_hash,
tr.from_address AS `from`,
tr.to_address AS `to`,
SPLIT(tr.trace_address,',') AS trace_address,
tr.trace_type AS type, --BQ has additional types: genesis, daofork	
CASE tr.trace_type 
  WHEN 'suicide' THEN tr.to_address
  WHEN 'create' THEN tr.to_address
  ELSE NULL
  END
  AS address,
CASE tr.trace_type
  WHEN 'create' THEN tr.input
  ELSE NULL
  END 
  AS code,
CASE tr.call_type
  WHEN 'callcode' THEN 'call'
  WHEN 'delegatecall' THEN 'delegatecall'
  WHEN 'staticcall' THEN 'staticcall'
  END AS call_type,
tr.input,
tr.output,
CASE tr.trace_type
  WHEN 'suicide' THEN tr.to_address
  ELSE NULL
  END AS refund_address,

-- Fields absent in Dune
tr.reward_type,
tr.trace_id,
tr.status, -- apparently related to success and tx_success
LEFT(tr.input, 10) as method_id,
LEFT(tx.input,10) as tx_method_id
FROM {{ source('crypto_ethereum', 'traces') }}  AS tr
LEFT JOIN {{ source('crypto_ethereum', 'transactions') }} AS tx
  ON tx.hash = tr.transaction_hash
{% if is_incremental() %}
WHERE tr.block_timestamp > current_timestamp() - interval {{ var('decode_source_interval') }}
and tx.block_timestamp > current_timestamp() - interval {{ var('decode_source_interval') }}
{% endif %}