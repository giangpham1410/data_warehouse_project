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
    , is_salesperson
    , is_employee
  FROM dim_person__source 
)

, dim_person__cast_type AS (
  SELECT
    CAST(person_key AS INTEGER) AS person_key
    , CAST(full_name AS STRING) AS full_name
    , CAST(preferred_name AS STRING) AS preferred_name
    , CAST(is_salesperson AS BOOLEAN) AS is_salesperson_boolean
    , CAST(is_employee AS BOOLEAN) AS is_employee_boolean
  FROM dim_person__rename_column
)

, dim_person__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_salesperson_boolean IS TRUE THEN 'Sales Person'
          WHEN is_salesperson_boolean IS FALSE THEN 'Not Sales Person'
          WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_salesperson
      , CASE
          WHEN is_employee_boolean IS TRUE THEN 'Employee'
          WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
          WHEN is_employee_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_employee
    FROM dim_person__cast_type
)

, dim_person__add_undefined_record AS (
    SELECT
      person_key
      , full_name
      , preferred_name
      , is_salesperson
      , is_employee
    FROM dim_person__convert_boolean

    UNION ALL

    SELECT 
      0 AS person_key
      , 'Undefined' AS full_name
      , 'Undefined' AS preferred_name
      , 'Undefined' AS is_salesperson
      , 'Undefined' AS is_employee

    UNION ALL

    SELECT 
      -1 AS person_key
      , 'Invalid' AS full_name
      , 'Invalid' AS preferred_name
      , 'Invalid' AS is_salesperson
      , 'Invalid' AS is_employee
)

SELECT
  person_key
  , full_name
  , preferred_name
  , is_salesperson
  , is_employee
FROM dim_person__add_undefined_record