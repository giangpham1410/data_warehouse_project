WITH
  dim_is_finalized AS (
    SELECT 'Finalized' AS is_finalized
    UNION ALL
    SELECT 'Not Finalized' AS is_finalized
)

SELECT
  FARM_FINGERPRINT(
    CONCAT(
      dim_is_finalized.is_finalized, '~'
      , dim_payment_method.payment_method_key, '~'
      , dim_transaction_type.transaction_type_key)
  ) AS transaction_indicator_key
  , is_finalized
  , dim_payment_method.payment_method_key
  , dim_payment_method.payment_method_name
  , dim_transaction_type.transaction_type_key
  , dim_transaction_type.transaction_type_name
FROM dim_is_finalized
  CROSS JOIN {{ ref('dim_transaction_type') }} dim_transaction_type
  CROSS JOIN {{ ref('stg_dim_payment_method') }} dim_payment_method