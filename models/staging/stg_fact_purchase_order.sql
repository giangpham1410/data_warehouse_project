WITH
  fact_purchase_order__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
)

, fact_purchase_order__rename_column AS (
    SELECT
      purchase_order_id AS purchase_order_key
      , is_order_finalized AS is_order_finalized_boolean
      , order_date
      , expected_delivery_date
      , supplier_reference
      , supplier_id AS supplier_key
      , delivery_method_id AS delivery_method_key
      , contact_person_id AS contact_person_key
    FROM fact_purchase_order__source
)

SELECT *
FROM fact_purchase_order__rename_column