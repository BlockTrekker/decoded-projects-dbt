{{ config(
    materialized='incremental',
    name = 'logs_clustered',
    schema = 'ethereum',
    cluster_by=['evt_hash', '`to`'],
)}}

{% if is_incremental() %}
-- Return no rows during incremental runs
SELECT * FROM {{ source('ethereum', 'logs') }} WHERE 1=0
{% else %}
SELECT
    *
FROM
    {{ source('ethereum', 'logs')}}
{% endif %}