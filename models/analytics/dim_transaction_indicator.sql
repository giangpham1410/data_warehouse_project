WITH
  dim_is_finalized AS (
    SELECT 'Finalized' AS is_finalized
    UNION ALL
    SELECT 'Not Finalized' AS is_finalized
)

SELECT is_finalized
FROM dim_is_finalized
  CROSS JOIN {{ ref('dim_transaction_type') }}
  CROSS JOIN {{ ref('stg_dim_payment_method') }}