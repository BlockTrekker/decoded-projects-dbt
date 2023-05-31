{{ config(
    materialized='table',
    schema='erc1155_ethereum',
    name='evt_URI',
)
}}
SELECT
    contract_address,
    udfs.hexToInt(topic1) as id,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    CAST(data as STRING) as `value`,
    evt_hash
FROM
    {{ source('ethereum','logs') }}
WHERE
    evt_hash = '0x6bb7ff708619ba0610cba295a58592e0451dee2622938c8755667688daf3529b'