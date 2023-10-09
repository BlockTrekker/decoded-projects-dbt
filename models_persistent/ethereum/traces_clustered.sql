
{{ config(
    materialized='incremental',
    cluster_by=['method_id', '`to`'],
    name = 'traces_clustered',
    schema = 'ethereum',
    tags='non_incremental'

)}}

SELECT
    *
FROM
    {{ source('ethereum', 'traces')}}
{% if is_incremental() %}
    -- Return no rows during incremental runs b/c it ruins the clustering
    WHERE block_time > current_timestamp()
    AND 1=0
{% endif %}