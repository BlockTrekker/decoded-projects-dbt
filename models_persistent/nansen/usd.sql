{{ config(
    materialized='incremental',
    schema='prices',
    name='usd',
    cluster_by='blockchain,contract_address'
)
}}

WITH five_min as (
  SELECT 
    pt.contract as contract_address,
    'ethereum' as blockchain,
    et.decimals,
    f.timestamp as minute,
    f.price_usd as price,
    pt.symbol
  FROM `nansen-query.tokens_ethereum.5m_token_prices_coinpaprika` f
  LEFT JOIN `nansen-query.tokens_ethereum.tokens_coinpaprika` pt
    ON pt.id = f.coin_id
  LEFT JOIN `nansen-query.tokens_ethereum.tokens` et
    ON et.address = pt.contract
  WHERE pt.contract is not null     
  {% if is_incremental() %}
  AND f.timestamp > (current_timestamp() - INTERVAL 1 day)
  {% endif %}
)

SELECT
  *
FROM five_min
UNION ALL
SELECT
  contract_address,
  blockchain,
  decimals,
  TIMESTAMP_ADD(minute, INTERVAL 1 MINUTE) as minute,
  price,
  symbol
FROM five_min
WHERE minute < current_timestamp()
UNION ALL
SELECT
  contract_address,
  blockchain,
  decimals,
  TIMESTAMP_ADD(minute, INTERVAL 2 MINUTE) as minute,
  price,
  symbol
FROM five_min
WHERE minute < current_timestamp()
UNION ALL
SELECT
  contract_address,
  blockchain,
  decimals,
  TIMESTAMP_ADD(minute, INTERVAL 3 MINUTE) as minute,
  price,
  symbol
FROM five_min
WHERE minute < current_timestamp()
UNION ALL
SELECT
  contract_address,
  blockchain,
  decimals,
  TIMESTAMP_ADD(minute, INTERVAL 4 MINUTE) as minute,
  price,
  symbol
FROM five_min
WHERE minute < current_timestamp()