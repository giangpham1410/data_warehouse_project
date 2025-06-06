WITH
  dim_product__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column AS (
    SELECT
      stock_item_id AS product_key
      , stock_item_name AS product_name
      , brand AS brand_name
      , supplier_id AS supplier_key
      , is_chiller_stock
    FROM dim_product__source
)

, dim_product__cast_type AS (
    SELECT
      CAST(product_key AS INTEGER) AS product_key
      , CAST(product_name AS STRING) AS product_name
      , CAST(brand_name AS STRING) AS brand_name
      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(is_chiller_stock AS BOOLEAN) AS is_chiller_stock_boolean
    FROM dim_product__rename_column
)

, dim_product__convert_boolean AS (
    SELECT 
      *
      , CASE
          WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
          WHEN is_chiller_stock_boolean IS FALSE THEN 'Chiller Stock'
          WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
          END AS is_chiller_stock
    FROM dim_product__cast_type
)

, dim_product__add_undefined_record AS (
    SELECT
      product_key
      , product_name
      , brand_name
      , supplier_key
      , is_chiller_stock
    FROM dim_product__convert_boolean

    UNION ALL

    SELECT
      0 AS product_key
      , 'Undefined' AS product_name
      , 'Undefined' AS brand_name
      , 0 AS supplier_key
      , 'Undefined' AS is_chiller_stock

    UNION ALL

    SELECT
      -1 AS product_key
      , 'Invalid' AS product_name
      , 'Invalid' AS brand_name
      , -1 AS supplier_key
      , 'Invalid' AS is_chiller_stock
)

, dim_product__handle_null AS (
    SELECT
      product_key
      , product_name
      , COALESCE(brand_name, 'Undefined') AS brand_name
      , COALESCE(supplier_key, 0) AS supplier_key
      , COALESCE(is_chiller_stock, 'Undefined') AS is_chiller_stock
    FROM dim_product__add_undefined_record
)

SELECT
  dim_product.product_key
  , dim_product.product_name

  , dim_product.brand_name
  , dim_product.is_chiller_stock
  
  , dim_product.supplier_key
  , dim_supplier.supplier_name
FROM dim_product__handle_null dim_product
  LEFT JOIN {{ ref('dim_supplier') }} dim_supplier
    ON dim_product.supplier_key = dim_supplier.supplier_key
