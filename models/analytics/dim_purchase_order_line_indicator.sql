WITH 
  dim_delivery_method AS (
    SELECT
      delivery_method_key
      , delivery_method_name
    FROM {{ ref ("stg_dim_delivery_method") }}
)

, dim_package_type AS (
    SELECT
      package_type_key
      , package_type_name
    FROM {{ ref('stg_dim_package_type') }}
)

, dim_is_order_line_finalized AS (
    SELECT
      'Order Line Finalized' AS is_order_line_finalized
    UNION ALL
    SELECT
      'Not Order Line Finalized' AS is_order_line_finalized
)

, dim_is_order_finalized AS (
    SELECT
      'Order Finalized' AS is_order_finalized
    UNION ALL
    SELECT
      'Not Order Finalized' AS is_order_finalized
)

SELECT
  FARM_FINGERPRINT (
    CONCAT(
      delivery_method_key
      ,'~', package_type_key
      ,'~', is_order_line_finalized
      ,'~', is_order_finalized
      )
   ) AS purchase_order_line_indicator_key
  , delivery_method_key
  , delivery_method_name
  , package_type_key
  , package_type_name
  , is_order_line_finalized
  , is_order_finalized
FROM dim_delivery_method
  CROSS JOIN dim_package_type
  CROSS JOIN dim_is_order_line_finalized
  CROSS JOIN dim_is_order_finalized