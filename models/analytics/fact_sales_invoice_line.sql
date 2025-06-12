WITH
  fact_invoice_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__invoice_lines`
)

, fact_invoice_line__rename_column AS (
    SELECT
      invoice_line_id AS sales_invoice_line_key
      , description
      , quantity
      , unit_price
      , tax_rate
      , tax_amount
      , line_profit
      , extended_price

      , stock_item_id AS product_key
      , package_type_id AS package_type_key
      , invoice_id AS sales_invoice_key
    FROM fact_invoice_line__source
)

, fact_invoice_line__cast_type AS (
    SELECT
      CAST(sales_invoice_line_key AS INTEGER) AS sales_invoice_line_key
      , CAST(description AS STRING) AS description
      , CAST(quantity AS INTEGER) AS quantity
      , CAST(unit_price AS NUMERIC) AS unit_price
      , CAST(tax_rate AS NUMERIC) AS tax_rate
      , CAST(tax_amount AS NUMERIC) AS tax_amount
      , CAST(line_profit AS NUMERIC) AS line_profit
      , CAST(extended_price AS NUMERIC) AS extended_price

      , CAST(product_key AS INTEGER) AS product_key
      , CAST(package_type_key AS INTEGER) AS package_type_key
      , CAST(sales_invoice_key AS INTEGER) AS sales_invoice_key
    FROM fact_invoice_line__rename_column
)

, fact_invoice_line__handle_null AS (
    SELECT
      sales_invoice_line_key
      , description
      , quantity
      , unit_price
      , tax_rate
      , tax_amount
      , line_profit
      , extended_price

      , COALESCE(product_key, 0) AS product_key
      , COALESCE(package_type_key, 0) AS package_type_key
      , COALESCE(sales_invoice_key, 0) AS sales_invoice_key
    FROM fact_invoice_line__cast_type
)

SELECT
  fact_invoice_line.sales_invoice_line_key
  , fact_invoice_line.description
  , fact_invoice_line.quantity
  , fact_invoice_line.unit_price
  , fact_invoice_line.tax_rate
  , fact_invoice_line.tax_amount
  , fact_invoice_line.line_profit
  , fact_invoice_line.extended_price
  , fact_invoice.is_credit_note
  , fact_invoice.total_invoice_dy_items
  , fact_invoice.total_invoice_chiller_items
  , fact_invoice.customer_purchase_order_number
  , fact_invoice.returned_delivery_data
  , fact_invoice.invoice_date
  , fact_invoice.confirmed_delivery_date
  , fact_invoice.confirmed_received_by

  , fact_invoice_line.product_key
  , fact_invoice_line.package_type_key
  , COALESCE(fact_invoice.customer_key, -1) AS customer_key
  , COALESCE(fact_invoice.bill_to_customer_key, -1) AS bill_to_customer_key
  , COALESCE(fact_invoice.sales_order_key, -1) AS sales_order_key
  , COALESCE(fact_invoice.delivery_method_key, -1) AS delivery_method_key
  , COALESCE(fact_invoice.contact_person_key, -1) AS contact_person_key
  , COALESCE(fact_invoice.accounts_person_key, -1) AS accounts_person_key
  , COALESCE(fact_invoice.salesperson_person_key, -1) AS salesperson_person_key
  , COALESCE(fact_invoice.packed_by_person_key, -1) AS packed_by_person_key

  , fact_invoice_line.sales_invoice_key
FROM fact_invoice_line__handle_null fact_invoice_line
  LEFT JOIN {{ ref('stg_fact_sales_invoice') }} fact_invoice ON fact_invoice_line.sales_invoice_key = fact_invoice.sales_invoice_key