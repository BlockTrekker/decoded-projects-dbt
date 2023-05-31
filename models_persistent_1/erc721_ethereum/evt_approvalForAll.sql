{{ config(
    materialized='table',
    schema='erc721_ethereum',
    name='evt_ApprovalForAll',
)
}}

SELECT
    topic3 as approved,
    contract_address,
    topic2 as operator,
    topic1 as `owner`,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    evt_hash
FROM
    {{ source('ethereum','logs') }}
WHERE
    evt_hash = '0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31'