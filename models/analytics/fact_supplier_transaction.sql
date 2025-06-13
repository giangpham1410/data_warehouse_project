WITH
  fact_supplier_transaction__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__supplier_transactions`
)

, fact_supplier_transaction__rename_column AS (
    SELECT
      supplier_transaction_id AS supplier_transaction_key
      , supplier_invoice_number
      , amount_excluding_tax
      , tax_amount
      , transaction_amount
      , outstanding_balance
      , is_finalized AS is_supplier_finalized
      , transaction_date
      , finalization_date
      , supplier_id AS supplier_id 
      , transaction_type_id AS transaction_type_key
      , purchase_order_id AS purchase_order_key
      , payment_method_id AS payment_method_key
    FROM fact_supplier_transaction__source
)

SELECT *
FROM fact_supplier_transaction__rename_column