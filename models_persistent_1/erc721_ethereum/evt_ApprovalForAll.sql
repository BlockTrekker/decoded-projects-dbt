{{ config(
    materialized='table',
    schema='erc721_ethereum',
    name='evt_ApprovalForAll',
)
}}

SELECT
    CONCAT('0x', RIGHT(topic3, 40)) as approved,
    contract_address,
    CONCAT('0x', RIGHT(topic2, 40)) as operator,
    CONCAT('0x', RIGHT(topic1, 40)) as `owner`,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    evt_hash
FROM
    {{ source('ethereum','logs') }}
WHERE
    evt_hash = '0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31'
AND
    contract_address in (SELECT contract_address FROM {{ source('ethereum', 'token_types') }} WHERE erc721 is true)