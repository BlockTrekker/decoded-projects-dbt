{{ config(
    materialized='incremental',
    schema='erc1155_ethereum',
    name='evt_URI',
)
}}

-- depends_on: {{ source('ethereum','one_day_logs') }}
-- depends_on: {{ source('ethereum','logs_clustered') }}

SELECT
    contract_address,
    udfs.hexToInt(topic1) as id,
    block_number as evt_block_number,
    block_time as evt_block_time,
    index as evt_index,
    tx_hash as evt_tx_hash,
    SAFE_CAST(data as STRING) as `value`,
    evt_hash
FROM
{% if is_incremental() %}
    {{ source('ethereum','one_day_logs') }}
{% else %}
    {{ source('ethereum','logs_clustered') }}
{% endif %}
WHERE
    evt_hash = '0x6bb7ff708619ba0610cba295a58592e0451dee2622938c8755667688daf3529b'
AND
    contract_address in (SELECT contract_address FROM {{ source('ethereum', 'token_types') }} WHERE erc1155 is true)
{% if is_incremental() %}
AND 
    block_time > (current_timestamp() - INTERVAL 1 day)
{% endif %}    