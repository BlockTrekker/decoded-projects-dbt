{{ config(
    materialized='incremental',
    schema='erc721_ethereum',
    name='evt_Transfer',
)
}}

-- depends_on: {{ source('ethereum','one_day_logs') }}
-- depends_on: {{ source('ethereum','logs_clustered') }}

SELECT
    contract_address,
    CONCAT('0x', RIGHT(topic1,40)) as `from`,
    CONCAT('0x', RIGHT(topic2,40)) as `to`,
    SAFE_CAST(udfs.hexToInt(topic3) as INT) as tokenId,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    evt_hash
FROM
{% if is_incremental() %}
    {{ source('ethereum','one_day_logs') }}
{% else %}
    {{ source('ethereum','logs_clustered') }}
{% endif %}
WHERE
    evt_hash = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
AND
    contract_address in (SELECT contract_address FROM {{ source('ethereum', 'token_types') }} WHERE erc721 is true)
{% if is_incremental() %}
AND 
    block_time > (current_timestamp() - INTERVAL 1 day)
{% endif %}