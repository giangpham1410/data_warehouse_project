WITH
  dim_supplier_category__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
)

, dim_supplier_category__rename_column AS (
  SELECT
    supplier_category_id AS supplier_category_key
    , supplier_category_name
  FROM dim_supplier_category__source
)

, dim_supplier_category__cast_type AS (
    SELECT
      CAST(supplier_category_key AS INTEGER) AS supplier_category_key
      , CAST(supplier_category_name AS STRING) AS supplier_category_name
    FROM dim_supplier_category__rename_column
)

, dim_supplier_category__add_undefined_record AS (
    SELECT
      supplier_category_key
      , supplier_category_name
    FROM dim_supplier_category__cast_type

    UNION ALL

    SELECT
      0 AS supplier_category_key
      , 'Undefined' AS supplier_category_name

    UNION ALL

    SELECT
      -1 AS supplier_category_key
      , 'Invalid' AS supplier_category_name
)

SELECT
  supplier_category_key
  , supplier_category_name
FROM dim_supplier_category__add_undefined_record