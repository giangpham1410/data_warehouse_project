WITH
  fact_invoice__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__invoices`
)

, fact_invoice__rename_column AS (
    SELECT
      invoice_id AS invoice_key
      , is_credit_note
      , total_dry_items AS total_invoice_dy_items
      , total_chiller_items AS total_invoice_chiller_items
      , invoice_date
      , confirmed_delivery_time AS confirmed_delivery_date
      , confirmed_received_by
      , returned_delivery_data
      , customer_id AS customer_key
      , bill_to_customer_id AS bill_to_customer_key
      , order_id AS sales_order_key
      , delivery_method_id AS delivery_method_key
      , contact_person_id AS contact_person_key
      , accounts_person_id AS accounts_person_key
      , salesperson_person_id AS salesperson_person_key
      , packed_by_person_id AS packed_by_person_key
      , customer_purchase_order_number
    FROM fact_invoice__source
)

SELECT  *
FROM fact_invoice__rename_column