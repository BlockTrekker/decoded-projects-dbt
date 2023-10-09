
{{ config(
    materialized='incremental',
    cluster_by=['method_id', '`to`'],
    name = 'traces_clustered',
    schema = 'ethereum'
)}}

SELECT
    *
FROM
    {{ source('ethereum', 'traces')}}
{% if is_incremental() %}
    -- Return no rows during incremental runs b/c it ruins the clustering
    WHERE 1=0
{% endif %}