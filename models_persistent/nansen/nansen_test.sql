{{ config(
    materialized='view',
    schema='prices',
    name='usd',
)
}}

SELECT `hash` FROM `nansen-query.raw_arbitrum.blocks` LIMIT 1000