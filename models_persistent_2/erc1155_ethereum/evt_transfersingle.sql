{{ config(
    materialized='table',
    schema='erc1155_ethereum',
    name='evt_Transfersingle',
)
}}
SELECT
    contract_address,
    topic2 as `from`,
    udfs.hexToInt(LEFT(data, 66)) as id,
    topic1 as operator,
    topic3 as `to`,
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