WITH
  dim_person__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS (
  SELECT
    person_id AS person_key
    , full_name
    , preferred_name
  FROM dim_person__source 
)

, dim_person__cast_type AS (
  SELECT
    CAST(person_key AS INTEGER) AS person_key
    , CAST(full_name AS STRING) AS full_name
    , CAST(preferred_name AS STRING) AS preferred_name
  FROM dim_person__rename_column
)

, dim_person__add_undefined_record AS (
    SELECT
      person_key
      , full_name
      , preferred_name
    FROM dim_person__cast_type

    UNION ALL

    SELECT 
      0 AS person_key
      , 'Undefined' AS full_name
      , 'Undefined' AS preferred_name

    UNION ALL

    SELECT 
      -1 AS person_key
      , 'Invalid' AS full_name
      , 'Invalid' AS preferred_name
)

SELECT
  person_key
  , full_name
  , preferred_name
FROM dim_person__add_undefined_record