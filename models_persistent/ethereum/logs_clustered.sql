{{ config(
    materialized='incremental',
    name = 'logs_clustered',
    schema = 'ethereum',
    cluster_by=['evt_hash', '`to`'],
    tags='non_incremental'

)}}

SELECT * FROM {{ source('ethereum', 'logs') }}

{% if is_incremental() %}
    -- Return no rows during incremental runs b/c it ruins the clustering.  Just a safety mechanism
    WHERE block_time > current_timestamp()
    AND 1=0
{% else %}
{% endif %}