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
      , customer_id AS customer_key
      , transaction_type_id AS transaction_type_key
      , invoice_id AS sales_invoice_key
      , payment_method_id AS payment_method_key
    FROM fact_customer_transaction__source
)

SELECT *
FROM fact_customer_transaction__rename_column