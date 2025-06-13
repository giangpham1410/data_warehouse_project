WITH
  dim_payment_method__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__payment_methods`
)

, dim_payment_method__rename_column AS (
    SELECT
      payment_method_id AS payment_method_key
      , payment_method_name
    FROM dim_payment_method__source
)

, dim_payment_method__cast_type AS (
    SELECT
      CAST(payment_method_key AS INTEGER) AS payment_method_key
      , CAST(payment_method_name AS STRING) AS payment_method_name
    FROM dim_payment_method__rename_column
)

SELECT *
FROM dim_payment_method__cast_type