{{ config(
    materialized='table',
    schema='erc721_ethereum',
    name='evt_Approval',
)
}}

SELECT
    topic2 as approved,
    contract_address,
    topic1 as `owner`,
    udfs.hexToInt(topic3) as tokenId,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    evt_hash
FROM
    {{ source('ethereum','logs') }}
WHERE
    evt_hash = '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'