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
    FROM dim_product__source
)

, dim_product__cast_type AS (
    SELECT
      CAST(product_key AS INTEGER) AS product_key
      , CAST(product_name AS STRING) AS product_name
      , CAST(brand_name AS STRING) AS brand_name
      , CAST(supplier_key AS INTEGER) AS supplier_key
    FROM dim_product__rename_column
)

SELECT
  product.product_key
  , product.product_name
  , product.brand_name
  , product.supplier_key
  , supplier.supplier_name
FROM dim_product__cast_type product
  LEFT JOIN `data-warehouse-project-459212.wide_world_importers_dwh.dim_supplier` supplier
    ON product.supplier_key = supplier.supplier_key
