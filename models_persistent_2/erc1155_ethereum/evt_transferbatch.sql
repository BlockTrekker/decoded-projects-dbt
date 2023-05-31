{{ config(
    materialized='table',
    schema='erc1155_ethereum',
    name='evt_Transferbatch',
)
}}
SELECT
    contract_address,
    topic2 as `from`,
    udfs.decodeTransferBatchIds(data) as ids,
    topic1 as operator,
    topic3 as `to`,
    udfs.decodeTransferBatchValues(data) as values,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    evt_hash
FROM
    {{ source('ethereum','logs') }}
WHERE
    evt_hash = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'