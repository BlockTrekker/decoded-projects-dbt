{{ config(
    materialized='incremental',
    unique_key=['contract_address'],
    schema='ethereum',
    name='token_types',
)
}}
WITH standards AS (
  SELECT 
    address as contract_address,
    FALSE as erc20,
    TRUE as erc721,
    FALSE as erc1155
  FROM
    {{ source('crypto_ethereum','contracts') }} 
  WHERE bytecode like '%6370a08231%'   -- balanceOf(address)
  AND bytecode like '%636352211e%'  -- ownerOf(ut256)
  AND bytecode like '%63b88d4fde%'  -- safeTransferFrom(address,address,ut256,bytes)
  AND bytecode like '%6342842e0e%'  -- safeTransferFrom(address,address,ut256)
  AND bytecode like '%6323b872dd%'  -- transferFrom(address,address,ut256)
  AND bytecode like '%63095ea7b3%'  -- approve(address,ut256)
  AND bytecode like '%63a22cb465%'  -- setApprovalForAll(address,bool)
  AND bytecode like '%63e985e9c5%'  -- isApprovedForAll(address,address)
  {% if is_incremental() %}
    AND
    block_timestamp > (current_timestamp() - INTERVAL 1 day)
  {% endif %}
  UNION ALL
  SELECT 
    address as contract_address,
    TRUE as erc20,
    FALSE as erc721,
    FALSE as erc1155
  FROM
    {{ source('crypto_ethereum','contracts') }}
  WHERE bytecode like '%6318160ddd%'   -- totalSupply()
  AND bytecode like '%6370a08231%'  -- balanceOf(address)
  AND bytecode like '%63a9059cbb%'  -- transfer(address,uint256)
  AND bytecode like '%63095ea7b3%'  -- approve(address,uint256)
  AND bytecode like '%63dd62ed3e%'  -- allowance(address,address)
  AND bytecode like '%6323b872dd%'  -- transferFrom(address,address,uint256)
  {% if is_incremental() %}
    AND
    block_timestamp > (current_timestamp() - INTERVAL 1 day)
  {% endif %}

  UNION ALL
  SELECT 
    address as contract_address,
    FALSE as erc20,
    FALSE as erc721,
    TRUE as erc1155
  FROM
    {{ source('crypto_ethereum','contracts') }}
  WHERE bytecode like '%62fdd58e%'   -- balanceOf(address,uint256)
  AND bytecode like '%634e1273f4%'  -- balanceOfBatch(address[],uint256[])
  AND bytecode like '%63f242432a%'  -- safeTransferFrom(address,address,uint256,uint256,bytes)
  AND bytecode like '%632eb2c2d6%'  -- safeBatchTransferFrom(address,address,uint256[],uint256[],bytes)
  AND bytecode like '%63a22cb465%'  -- setApprovalForAll(address,bool)
  AND bytecode like '%63e985e9c5%'  -- isApprovedForAll(address,address)
  {% if is_incremental() %}
    AND
    block_timestamp > (current_timestamp() - INTERVAL 1 day)
  {% endif %}
)

SELECT
  contract_address,
  MAX(erc20) as erc20,
  MAX(erc721) as erc721,
  MAX(erc1155) as erc1155,
FROM standards
GROUP BY
  contract_address
ORDER BY erc20, erc721, erc1155
