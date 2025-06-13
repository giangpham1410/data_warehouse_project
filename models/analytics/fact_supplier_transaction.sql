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
      , supplier_id AS supplier_key
      , transaction_type_id AS transaction_type_key
      , purchase_order_id AS purchase_order_key
      , payment_method_id AS payment_method_key
    FROM fact_supplier_transaction__source
)

, fact_supplier_transaction__cast_type AS (
    SELECT
      CAST(supplier_transaction_key AS INTEGER) AS supplier_transaction_key
      , CAST(supplier_invoice_number AS STRING) AS supplier_invoice_number
      , CAST(amount_excluding_tax AS NUMERIC) AS amount_excluding_tax
      , CAST(tax_amount AS NUMERIC) AS tax_amount
      , CAST(transaction_amount AS NUMERIC) AS transaction_amount
      , CAST(outstanding_balance AS NUMERIC) AS outstanding_balance
      , CAST(is_supplier_finalized AS BOOLEAN) AS is_supplier_finalized_boolean
      , CAST(transaction_date AS DATE) AS transaction_date
      , CAST(finalization_date AS DATE) AS finalization_date
      , CAST(supplier_key  AS INTEGER) AS supplier_key
      , CAST(transaction_type_key AS INTEGER) AS transaction_type_key
      , CAST(purchase_order_key AS INTEGER) AS purchase_order_key
      , CAST(payment_method_key AS INTEGER) AS payment_method_key
    FROM fact_supplier_transaction__rename_column
)

SELECT *
FROM fact_supplier_transaction__cast_type