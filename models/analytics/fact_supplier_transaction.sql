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
      , CAST(is_supplier_finalized AS BOOLEAN) AS is_supplier_finalized_boolean
      , CAST(amount_excluding_tax AS NUMERIC) AS amount_excluding_tax
      , CAST(tax_amount AS NUMERIC) AS tax_amount
      , CAST(transaction_amount AS NUMERIC) AS transaction_amount
      , CAST(outstanding_balance AS NUMERIC) AS outstanding_balance
      , CAST(transaction_date AS DATE) AS transaction_date
      , CAST(finalization_date AS DATE) AS finalization_date
      , CAST(supplier_key  AS INTEGER) AS supplier_key
      , CAST(transaction_type_key AS INTEGER) AS transaction_type_key
      , CAST(purchase_order_key AS INTEGER) AS purchase_order_key
      , CAST(payment_method_key AS INTEGER) AS payment_method_key
    FROM fact_supplier_transaction__rename_column
)

, fact_supplier_transaction__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_supplier_finalized_boolean IS TRUE THEN 'Finalized'
          WHEN is_supplier_finalized_boolean IS FALSE THEN 'Not Finalized'
          WHEN is_supplier_finalized_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_supplier_finalized
    FROM fact_supplier_transaction__cast_type
)

, fact_supplier_transaction__handle_null AS (
    SELECT
      supplier_transaction_key
      , COALESCE(supplier_invoice_number, 'Undefined') AS supplier_invoice_number
      , is_supplier_finalized
      , amount_excluding_tax
      , tax_amount
      , transaction_amount
      , outstanding_balance
      , transaction_date
      , finalization_date
      , COALESCE(supplier_key, 0) AS supplier_key
      , COALESCE(transaction_type_key, 0) AS transaction_type_key
      , COALESCE(purchase_order_key, 0) AS purchase_order_key
      , COALESCE(payment_method_key, 0) AS payment_method_key
    FROM fact_supplier_transaction__convert_boolean
)

SELECT
  supplier_transaction_key
  , supplier_invoice_number
  , is_supplier_finalized
  , amount_excluding_tax
  , tax_amount
  , transaction_amount
  , outstanding_balance
  , transaction_date
  , finalization_date
  , supplier_key
  , transaction_type_key
  , purchase_order_key
  , payment_method_key
FROM fact_supplier_transaction__handle_null