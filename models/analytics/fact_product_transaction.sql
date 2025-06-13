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

SELECT *
FROM fact_product_transaction__rename_column