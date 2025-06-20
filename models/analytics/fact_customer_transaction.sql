WITH
  fact_customer_transaction__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customer_transactions`
)

, fact_customer_transaction__rename_column AS (
    SELECT
      customer_transaction_id AS customer_transaction_key
      , is_finalized AS is_customer_finalized
      , amount_excluding_tax
      , tax_amount
      , transaction_amount
      , outstanding_balance
      , transaction_date
      , finalization_date
      , invoice_id AS sales_invoice_key
      , customer_id AS customer_key
      , transaction_type_id AS transaction_type_key
      , payment_method_id AS payment_method_key
    FROM fact_customer_transaction__source
)

, fact_customer_transaction__cast_type AS (
    SELECT
      CAST(customer_transaction_key AS INTEGER) AS customer_transaction_key
      , CAST(is_customer_finalized AS BOOLEAN) AS is_customer_finalized_boolean
      , CAST(amount_excluding_tax AS NUMERIC) AS amount_excluding_tax
      , CAST(tax_amount AS NUMERIC) AS tax_amount
      , CAST(transaction_amount AS NUMERIC) AS transaction_amount
      , CAST(outstanding_balance AS NUMERIC) AS outstanding_balance
      , CAST(transaction_date AS DATE) AS transaction_date
      , CAST(finalization_date AS DATE) AS finalization_date
      , CAST(sales_invoice_key AS INTEGER) AS sales_invoice_key
      , CAST(customer_key AS INTEGER) AS customer_key
      , CAST(transaction_type_key AS INTEGER) AS transaction_type_key
      , CAST(payment_method_key AS INTEGER) AS payment_method_key
    FROM fact_customer_transaction__rename_column
)

, fact_customer_transaction__convert_boolean AS (
    SELECT
      *
      , CASE
          WHEN is_customer_finalized_boolean IS TRUE THEN 'Finalized'
          WHEN is_customer_finalized_boolean IS FALSE THEN 'Not Finalized'
          WHEN is_customer_finalized_boolean IS NULL THEN 'Undefined'
          ELSE 'Invalid'
        END AS is_customer_finalized
    FROM fact_customer_transaction__cast_type
)

, fact_customer_transaction__handle_null AS (
    SELECT
      customer_transaction_key
      , is_customer_finalized
      , amount_excluding_tax
      , tax_amount
      , transaction_amount
      , outstanding_balance
      , transaction_date
      , finalization_date
      , sales_invoice_key
      , COALESCE(customer_key, 0) AS customer_key
      , COALESCE(transaction_type_key, 0) AS transaction_type_key
      , COALESCE(payment_method_key, 0) AS payment_method_key
    FROM fact_customer_transaction__convert_boolean
)

SELECT
  customer_transaction_key
  --, is_customer_finalized
  , amount_excluding_tax
  , tax_amount
  , transaction_amount
  , outstanding_balance
  , transaction_date
  , finalization_date
  , sales_invoice_key
  , customer_key
  --, transaction_type_key
  --, payment_method_key
  , FARM_FINGERPRINT(
    CONCAT(
      is_customer_finalized, '~'
      , payment_method_key, '~'
      , transaction_type_key
    )
  ) AS transaction_indicator_key
FROM fact_customer_transaction__handle_null
