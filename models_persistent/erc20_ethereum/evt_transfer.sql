{{ config(
    materialized='table',
    schema='erc20_ethereum',
    name='evt_Transfer',
)
}}


SELECT
    contract_address,
    topic1 as `from`,
    topic2 as `to`,
    udfs.hexToInt(data) as `value`,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    evt_hash
FROM
    {{ source('ethereum','logs') }}
WHERE
    evt_hash = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    