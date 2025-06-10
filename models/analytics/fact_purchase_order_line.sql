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

, fact_purchase_order_line__handle_null AS (
    SELECT
      purchase_order_line_key
      , description
      , is_order_line_finalized
      , ordered_outers
      , received_outers
      , expected_unit_price_per_outer
      , last_receipt_date
      , COALESCE(product_key, 0) AS product_key
      , COALESCE(package_type_key, 0) AS package_type_key
      , COALESCE(purchase_order_key, 0) AS purchase_order_key
    FROM fact_purchase_order_line__convert_boolean
)

SELECT
    fact_purchase_order_line.purchase_order_line_key
  , fact_purchase_order_line.description
  --, fact_purchase_order_line.is_order_line_finalized
  --, fact_purchase_order.is_order_finalized
  , fact_purchase_order.supplier_reference
  , fact_purchase_order.order_date
  , fact_purchase_order.expected_delivery_date

  , fact_purchase_order_line.ordered_outers
  , fact_purchase_order_line.received_outers
  , fact_purchase_order_line.expected_unit_price_per_outer
  , fact_purchase_order_line.last_receipt_date

  , fact_purchase_order_line.product_key
  --, fact_purchase_order_line.package_type_key
  , fact_purchase_order_line.purchase_order_key

  , COALESCE(fact_purchase_order.supplier_key, -1) AS supplier_key
  --, COALESCE(fact_purchase_order.delivery_method_key, -1) AS delivery_method_key
  , COALESCE(fact_purchase_order.contact_person_key, -1) AS contact_person_key

  , CONCAT(
      fact_purchase_order.delivery_method_key
      ,'~', fact_purchase_order_line.package_type_key
      ,'~', fact_purchase_order_line.is_order_line_finalized
      ,'~', fact_purchase_order.is_order_finalized
  ) AS purchase_order_line_indicator_key

FROM fact_purchase_order_line__handle_null fact_purchase_order_line
  LEFT JOIN {{ ref("stg_fact_purchase_order") }} fact_purchase_order ON fact_purchase_order_line.purchase_order_key = fact_purchase_order.purchase_order_key