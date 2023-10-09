{{ config(
    materialized='incremental',
    schema='erc1155_ethereum',
    name='evt_ApprovalForAll',
)
}}

-- depends_on: {{ source('ethereum','one_day_logs') }}
-- depends_on: {{ source('ethereum','logs_clustered') }}


SELECT
    CONCAT('0x', RIGHT(topic1,40)) as account,
    contract_address,
    CONCAT('0x', RIGHT(topic2,40)) as operator,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    CONCAT('0x', RIGHT(topic3,40)) as approved,
    evt_hash
FROM
{% if is_incremental() %}
    {{ source('ethereum','one_day_logs') }}
{% else %}
    {{ source('ethereum','logs_clustered') }}
{% endif %}
WHERE
    evt_hash = '0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31'
AND
    contract_address in (SELECT contract_address FROM {{ source('ethereum', 'token_types') }} WHERE erc1155 is true)
{% if is_incremental() %}
AND 
    block_time > (current_timestamp() - INTERVAL 1 day)
{% endif %}    