WITH
  dim_is_sales_invoice_credit_note AS(
    SELECT
      'Credit Note' AS is_sales_invoice_credit_note
    UNION ALL
    SELECT
      'Not Credit Note' AS is_sales_invoice_credit_note
)

SELECT
  FARM_FINGERPRINT(
    CONCAT(dim_is_sales_invoice_credit_note.is_sales_invoice_credit_note, '~', dim_delivery_method.delivery_method_key, '~', dim_package_type.package_type_key)
   ) AS dim_sales_invoice_line_indicator
  , dim_is_sales_invoice_credit_note.is_sales_invoice_credit_note
  , dim_delivery_method.delivery_method_key
  , dim_delivery_method.delivery_method_name
  , dim_package_type.package_type_key
  , dim_package_type.package_type_name
FROM dim_is_sales_invoice_credit_note
  CROSS JOIN {{ ref('stg_dim_delivery_method') }} dim_delivery_method
  CROSS JOIN {{ ref('stg_dim_package_type') }} dim_package_type