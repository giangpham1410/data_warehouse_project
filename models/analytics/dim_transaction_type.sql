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

, dim_transaction_type__cast_type AS (
    SELECT
      CAST(transaction_type_key AS INTEGER) AS transaction_type_key
      , CAST(transaction_type_name AS STRING) AS transaction_type_name
    FROM dim_transaction_type__rename_column
)

, dim_transaction_type__add_undefined_record AS (
    SELECT
      transaction_type_key
      , transaction_type_name
    FROM dim_transaction_type__cast_type

    UNION ALL

    SELECT
      0 AS transaction_type_key
      , 'Undefined' AS transaction_type_name

    UNION ALL

    SELECT
      -1 AS transaction_type_key
      , 'Invalid' AS transaction_type_name
)

SELECT
  transaction_type_key
  , transaction_type_name
FROM dim_transaction_type__add_undefined_record