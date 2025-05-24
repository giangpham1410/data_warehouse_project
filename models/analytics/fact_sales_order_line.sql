WITH
  fact_sales_order_line__source AS (
    SELECT
      sales_line.*
      , sales_header.customer_id
    FROM `vit-lam-data.wide_world_importers.sales__order_lines` sales_line
      LEFT JOIN `vit-lam-data.wide_world_importers.sales__orders` sales_header ON sales_line.order_id = sales_header.order_id
)

, fact_sales_order_line__rename_column AS (
    SELECT
      order_line_id AS sales_order_line_key
      , stock_item_id AS product_key
      , customer_id AS customer_key
      , quantity
      , unit_price
    FROM fact_sales_order_line__source
)

SELECT *
FROM fact_sales_order_line__rename_column


/*
, fact_sales_order_line__rename_column AS (
  SELECT
    order_line_id AS sales_order_line_key
    , stock_item_id AS product_key
    , quantity
    , unit_price
  FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS (
    SELECT
      CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(quantity AS INTEGER) AS quantity
      , CAST(unit_price AS NUMERIC) AS unit_price
    FROM fact_sales_order_line__rename_column
)

, fact_sales_order_line__calculate_measure AS (
    SELECT
      *
      , quantity * unit_price AS gross_mount
    FROM fact_sales_order_line__cast_type
)

SELECT
  sales_order_line_key
  , product_key
  , quantity
  , unit_price
  , gross_mount
FROM fact_sales_order_line__calculate_measure
*/