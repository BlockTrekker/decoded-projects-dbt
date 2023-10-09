
{{ config(
    materialized='incremental',
    PartitionBy='TIMESTAMP_TRUNC(block_time, DAY)',
    name = 'logs',
    schema = 'ethereum'
)}}


SELECT 
  l.block_timestamp as block_time,
  l.block_number,
  l.block_hash,
  l.address as  contract_address,
  topics[SAFE_OFFSET(0)] as topic0,
  topics[SAFE_OFFSET(1)] as topic1,
  topics[SAFE_OFFSET(2)] as topic2,
  topics[SAFE_OFFSET(3)] as topic3,
  data,
  l.transaction_hash as tx_hash,
  log_index as index,
  l.transaction_index as tx_index,
  timestamp_trunc(l.block_timestamp, day) as block_date,
  from_address as `from`,
  to_address as `to`,
  topics[SAFE_OFFSET(0)] as evt_hash
FROM {{ source('goog_mainnet_ethereum', 'logs') }} l
LEFT JOIN {{ source('goog_mainnet_ethereum', 'transactions') }} t
  on t.transaction_hash = l.transaction_hash
{% if is_incremental() %}
WHERE l.block_timestamp > current_timestamp() - interval {{ var('decode_source_interval') }}
AND t.block_timestamp > current_timestamp() - interval {{ var('decode_source_interval') }}
{% endif %}
