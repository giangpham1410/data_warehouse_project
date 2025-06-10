WITH
  fact_purchase_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`
)

, fact_purchase_order_line__rename_column AS (
    SELECT
      purchase_order_line_id AS purchase_order_line_key
      , description
      , is_order_line_finalized
      , ordered_outers
      , received_outers
      , expected_unit_price_per_outer
      , last_receipt_date
      , stock_item_id AS product_key
      , package_type_id AS package_type_key
      , purchase_order_id AS purchase_order_key
    FROM fact_purchase_order_line__source
)

, fact_purchase_order_line__cast_type AS (
    SELECT
      purchase_order_line_key
      , CAST(description AS STRING) AS description
      , CAST(is_order_line_finalized AS BOOLEAN) AS is_order_line_finalized_boolean
      , CAST(ordered_outers AS INTEGER) AS ordered_outers
      , CAST(received_outers AS INTEGER) AS received_outers
      , CAST(expected_unit_price_per_outer AS NUMERIC) AS expected_unit_price_per_outer
      , CAST(last_receipt_date AS DATE) AS last_receipt_date
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(package_type_key AS INTEGER) AS package_type_key
      , CAST(purchase_order_key AS INTEGER) AS purchase_order_key
    FROM fact_purchase_order_line__rename_column
)

, fact_purchase_order_line__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_order_line_finalized_boolean IS TRUE THEN 'Order Line Finalized'
          WHEN is_order_line_finalized_boolean IS FALSE THEN 'Not Order Line Finalized'
          WHEN is_order_line_finalized_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_order_line_finalized
    FROM fact_purchase_order_line__cast_type
)


SELECT *
FROM fact_purchase_order_line__convert_boolean