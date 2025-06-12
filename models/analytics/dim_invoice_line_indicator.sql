WITH
  dim_is_invoice_credit_note AS(
    SELECT
      'Credit Note' AS is_invoice_credit_note
    UNION ALL
    SELECT
      'Not Credit Note' AS is_invoice_credit_note
)

, dim_delivery_method AS (
  SELECT
    delivery_method_key
    , delivery_method_name
  FROM {{ ref('stg_dim_delivery_method') }}
)

, dim_package_type AS (
    SELECT
      package_type_key
      , package_type_name
    FROM {{ ref('stg_dim_package_type') }}
)

SELECT
  FARM_FINGERPRINT(
    CONCAT(is_invoice_credit_note, '~', delivery_method_key, '~', package_type_key)
   ) AS dim_invoice_line_indicator
  , is_invoice_credit_note
  , delivery_method_key
  , delivery_method_name
  , package_type_key
  , package_type_name
FROM dim_is_invoice_credit_note
  CROSS JOIN dim_delivery_method
  CROSS JOIN dim_package_type