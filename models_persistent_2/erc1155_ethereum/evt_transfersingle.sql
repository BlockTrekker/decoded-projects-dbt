{{ config(
    materialized='table',
    schema='erc1155_ethereum',
    name='evt_Transfersingle',
)
}}
SELECT
    contract_address,
    CONCAT('0x', RIGHT(topic2,40)) as `from`,
    SAFE_CAST(udfs.hexToInt(LEFT(data, 66)) as BIGNUMERIC) as id,
    CONCAT('0x', RIGHT(topic1,40)) as operator,
    CONCAT('0x', RIGHT(topic3,40)) as `to`,
    udfs.hexToInt(CONCAT("0x", RIGHT(data, 64))) as `value`,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    evt_hash
FROM
    {{ source('ethereum','logs') }}
WHERE
    evt_hash = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
AND
    contract_address in (SELECT contract_address FROM {{ source('ethereum', 'token_types') }} WHERE erc1155 is true)