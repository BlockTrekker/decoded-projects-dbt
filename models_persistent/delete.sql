{{ config(materialized='view') }}

WITH datasets_to_delete AS (
  SELECT
    schema_name
  FROM
    blocktrekker.INFORMATION_SCHEMA.SCHEMATA
  WHERE
    schema_name NOT IN ('decoded_contracts', 'decoded_projects', 'user_tables', 'dune_data', 'clustered_sources', 'udfs', 'the_graph_data')
)

SELECT
  CONCAT('DROP SCHEMA IF EXISTS `', schema_name, '` CASCADE;') AS statement
FROM
  datasets_to_delete