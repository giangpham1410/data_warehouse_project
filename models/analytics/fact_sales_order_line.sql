WITH
  fact_sales_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_column AS (
    SELECT
      order_line_id AS sales_order_line_key
      , description
      , picking_completed_when AS sales_order_line_picking_completed_when

      , unit_price
      , quantity
      , tax_rate
      , picked_quantity

      , stock_item_id AS product_key
      , package_type_id AS package_type_key
      , order_id AS sales_order_key
    FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS (
    SELECT
      CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
      , CAST(description AS STRING) AS description
      , CAST(sales_order_line_picking_completed_when AS DATE) AS sales_order_line_picking_completed_when

      , CAST(unit_price AS NUMERIC) AS unit_price
      , CAST(quantity AS INTEGER) AS quantity
      , CAST(tax_rate AS NUMERIC) AS tax_rate
      , CAST(picked_quantity AS INTEGER) AS picked_quantity
      
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(package_type_key AS INTEGER) AS package_type_key
      , CAST(sales_order_key AS INTEGER) AS sales_order_key
    FROM fact_sales_order_line__rename_column
)

, fact_sales_order_line__caculate_measure AS (
    SELECT
      *
      , COALESCE(quantity * unit_price, 0) AS gross_mount
    FROM fact_sales_order_line__cast_type
)

, fact_sales_order_line__handle_null AS (
    SELECT
      sales_order_line_key
      , description
      , sales_order_line_picking_completed_when
      , unit_price
      , quantity
      , tax_rate
      , picked_quantity
      , gross_mount
      , COALESCE(product_key, 0) AS product_key
      , COALESCE(package_type_key, 0) AS package_type_key
      , COALESCE(sales_order_key, 0) AS sales_order_key
    FROM fact_sales_order_line__caculate_measure
)

SELECT
    fact_so_line.sales_order_line_key
  , fact_so_line.description
  , fact_so_header.is_undersupply_backordered
  , fact_so_header.order_date
  , fact_so_header.expected_delivery_date
  , fact_so_header.sales_order_picking_completed_when
  , fact_so_line.sales_order_line_picking_completed_when
  , fact_so_line.unit_price
  , fact_so_line.quantity
  , fact_so_line.tax_rate
  , fact_so_line.picked_quantity
  , fact_so_line.gross_mount

  , fact_so_line.product_key
  , fact_so_line.package_type_key
  , fact_so_line.sales_order_key

  , COALESCE(fact_so_header.customer_key, -1) AS customer_key 
  , COALESCE(fact_so_header.salesperson_person_key, -1) AS salesperson_person_key 
  , COALESCE(fact_so_header.picked_by_person_key, -1) AS picked_by_person_key 
  , COALESCE(fact_so_header.contact_person_key, -1) AS contact_person_key 
  , COALESCE(fact_so_header.backorder_order_key, -1) AS backorder_order_key

  , FARM_FINGERPRINT(
      CONCAT(
          COALESCE(fact_so_header.is_undersupply_backordered, 'Undefined')
          , '~'
          , fact_so_line.package_type_key
        )
   ) AS sales_order_line_indicator_key

FROM fact_sales_order_line__handle_null fact_so_line
  LEFT JOIN {{ ref('stg_fact_sales_order') }} fact_so_header
    ON fact_so_line.sales_order_key = fact_so_header.sales_order_key

