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
      , size
      , is_chiller_stock
      , unit_price
      , recommended_retail_price
      , tax_rate
      , lead_time_days
      , quantity_per_outer
      , typical_weight_per_unit

      , supplier_id AS supplier_key
      , color_id AS color_key
      , unit_package_id AS unit_package_type_key
      , outer_package_id AS outer_package_type_key
    FROM dim_product__source
)

, dim_product__cast_type AS (
    SELECT
      CAST(product_key AS INTEGER) AS product_key
      , CAST(product_name AS STRING) AS product_name
      , CAST(brand_name AS STRING) AS brand_name
      , CAST(size AS STRING) AS size
      , CAST(is_chiller_stock AS BOOLEAN) AS is_chiller_stock_boolean

      , CAST(unit_price AS NUMERIC) AS unit_price
      , CAST(recommended_retail_price AS NUMERIC) AS recommended_retail_price
      , CAST(tax_rate AS NUMERIC) AS tax_rate
      , CAST(lead_time_days AS INTEGER) AS lead_time_days
      , CAST(quantity_per_outer AS INTEGER) AS quantity_per_outer
      , CAST(typical_weight_per_unit AS NUMERIC) AS typical_weight_per_unit

      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(color_key AS INTEGER) AS color_key
      , CAST(unit_package_type_key AS INTEGER) AS unit_package_type_key
      , CAST(outer_package_type_key AS INTEGER) AS outer_package_type_key
      
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
      , size
      , is_chiller_stock
      , unit_price
      , recommended_retail_price
      , tax_rate
      , lead_time_days
      , quantity_per_outer
      , typical_weight_per_unit

      , supplier_key
      , color_key
      , unit_package_type_key
      , outer_package_type_key
    FROM dim_product__convert_boolean

    UNION ALL

    SELECT
      0 AS product_key
      , 'Undefined' AS product_name
      , 'Undefined' AS brand_name
      , 'Undefined' AS size
      , 'Undefined' AS is_chiller_stock
      , 0 AS unit_price
      , 0 AS recommended_retail_price
      , 0 AS tax_rate
      , 0 AS lead_time_days
      , 0 AS quantity_per_outer
      , 0 AS typical_weight_per_unit
      
      , 0 AS supplier_key
      , 0 AS color_key
      , 0 AS unit_package_type_key
      , 0 AS outer_package_type_key

    UNION ALL

    SELECT
      -1 AS product_key
      , 'Invalid' AS product_name
      , 'Invalid' AS brand_name
      , 'Invalid' AS size
      , 'Invalid' AS is_chiller_stock
      , -1 AS unit_price
      , -1 AS recommended_retail_price
      , -1 AS tax_rate
      , -1 AS lead_time_days
      , -1 AS quantity_per_outer
      , -1 AS typical_weight_per_unit

      , -1 AS supplier_key
      , -1 AS color_key
      , -1 AS unit_package_type_key
      , -1 AS outer_package_type_key
)


, dim_product__handle_null AS (
    SELECT
      product_key
      , product_name
      , COALESCE(brand_name, 'Undefined') AS brand_name 
      , COALESCE(size, 'Undefined') AS size 
      , COALESCE(is_chiller_stock, 'Undefined') AS is_chiller_stock
      
      , unit_price
      , recommended_retail_price AS recommended_retail_price
      , tax_rate
      , lead_time_days
      , quantity_per_outer
      , typical_weight_per_unit

      , COALESCE(supplier_key, 0) AS supplier_key 
      , COALESCE(color_key, 0) AS color_key 
      , COALESCE(unit_package_type_key, 0) AS unit_package_type_key 
      , COALESCE(outer_package_type_key, 0) AS outer_package_type_key
    FROM dim_product__add_undefined_record
)

SELECT
  dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_product.size
  , dim_product.is_chiller_stock
  , dim_product.unit_price
  , dim_product.recommended_retail_price
  , dim_product.tax_rate
  , dim_product.lead_time_days
  , dim_product.quantity_per_outer
  , dim_product.typical_weight_per_unit

  , dim_product.supplier_key
  , COALESCE(dim_supplier.supplier_name, 'Invalid') AS supplier_name 

  , dim_supplier_category.supplier_category_key
  , COALESCE(dim_supplier_category.supplier_category_name, 'Invalid') AS supplier_category_name 

  , dim_product.color_key
  , COALESCE(dim_color.color_name, 'Invalid') AS color_name 

  , dim_product.unit_package_type_key
  , COALESCE(dim_unit_package_type.package_type_name, 'Invalid') AS unit_package_type_name 

  , dim_product.outer_package_type_key
  , COALESCE(dim_outer_package_type.package_type_name, 'Invalid') AS outer_package_type_name 

FROM dim_product__handle_null dim_product
  LEFT JOIN {{ ref('dim_supplier') }} dim_supplier
    ON dim_product.supplier_key = dim_supplier.supplier_key

  LEFT JOIN {{ ref('stg_dim_supplier_category') }} dim_supplier_category
    ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key

  LEFT JOIN {{ ref('stg_dim_color')}} dim_color
    ON dim_product.color_key = dim_color.color_key

  LEFT JOIN {{ ref('stg_dim_package_type') }} dim_unit_package_type
    ON dim_product.unit_package_type_key = dim_unit_package_type.package_type_key

  LEFT JOIN {{ ref('stg_dim_package_type') }} dim_outer_package_type
    ON dim_product.outer_package_type_key = dim_outer_package_type.package_type_key
  
