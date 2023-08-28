{{ config(
    materialized='table',
    schema='prices',
    name='usd',
)
}}


SELECT 
  pt.contract as contract_address,
  'ethereum' as blockchain,
  et.decimals,
  f.timestamp as minute,
  f.price_usd as price,
  pt.symbol,
FROM `nansen-query.tokens_ethereum.5m_token_prices_coinpaprika` f
LEFT JOIN `nansen-query.tokens_ethereum.tokens_coinpaprika` pt
  ON pt.id = f.coin_id
LEFT JOIN `nansen-query.tokens_ethereum.tokens` et
  ON et.address = pt.contract
WHERE pt.contract is not null     
