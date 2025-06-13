WITH
  fact_product_transaction__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.warehouse__stock_item_transactions`
)

, fact_product_transaction__rename_column AS (
    SELECT
      stock_item_transaction_id AS product_transaction_key
      , quantity
      , transaction_occurred_when
      , stock_item_id AS product_key
      , transaction_type_id AS transaction_type_key
      , customer_id AS customer_key
      , invoice_id AS sales_invoice_key
      , supplier_id AS supplier_key
      , purchase_order_id AS purchase_order_key
    FROM fact_product_transaction__source
)

, fact_product_transaction__cast_type AS (
    SELECT
      CAST(product_transaction_key AS INTEGER) AS product_transaction_key
      , CAST(quantity AS NUMERIC) AS quantity
      , CAST(transaction_occurred_when AS DATE) AS transaction_occurred_when
      , CAST(product_key AS INTEGER) AS product_key
      , CAST(transaction_type_key AS INTEGER) AS transaction_type_key
      , CAST(customer_key AS INTEGER) AS customer_key
      , CAST(sales_invoice_key AS INTEGER) AS sales_invoice_key
      , CAST(supplier_key AS INTEGER) AS supplier_key
      , CAST(purchase_order_key AS INTEGER) AS purchase_order_key
    FROM fact_product_transaction__rename_column
)

SELECT *
FROM fact_product_transaction__cast_type