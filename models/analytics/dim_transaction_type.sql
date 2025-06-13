WITH
  dim_transaction_type__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__transaction_types`
)

, dim_transaction_type__rename_column AS (
    SELECT
      transaction_type_id AS transaction_type_key
      , transaction_type_name
    FROM dim_transaction_type__source
)

SELECT
  *
FROM dim_transaction_type__rename_column