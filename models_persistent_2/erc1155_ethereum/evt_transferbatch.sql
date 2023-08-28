{{ config(
    materialized='table',
    schema='erc1155_ethereum',
    name='evt_Transferbatch',
)
}}
SELECT
    contract_address,
    CONCAT('0x', RIGHT(topic2,40)) as `from`,
    ARRAY(SELECT CASE WHEN SAFE_CAST(id as BIGNUMERIC) is null then SAFE_CAST(0 AS BIGNUMERIC) else SAFE_CAST(id as BIGNUMERIC) end FROM UNNEST(udfs.decodeTransferBatchIds(data)) as id) as ids,
    CONCAT('0x', RIGHT(topic1,40)) as operator,
    CONCAT('0x', RIGHT(topic3,40)) as `to`,
    ARRAY(SELECT CASE WHEN SAFE_CAST(value as BIGNUMERIC) is null then SAFE_CAST(0 AS BIGNUMERIC) else SAFE_CAST(value as BIGNUMERIC) end FROM UNNEST(udfs.decodeTransferBatchValues(data)) as value) as values,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    evt_hash
FROM
    {{ source('ethereum','logs') }}
WHERE
    evt_hash = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
AND
    contract_address in (SELECT contract_address FROM {{ source('ethereum', 'token_types') }} WHERE erc1155 is true)
