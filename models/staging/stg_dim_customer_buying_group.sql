WITH
  dim_customer_buying_group__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, dim_customer_buying_group__rename_column AS (
  SELECT
    buying_group_id AS customer_buying_group_key
    , buying_group_name AS customer_buying_group_name
  FROM dim_customer_buying_group__source
)

, dim_customer_buying_group__cast_type AS (
  SELECT
    CAST(customer_buying_group_key AS INTEGER) AS customer_buying_group_key
    , CAST(customer_buying_group_name AS STRING) AS customer_buying_group_name
  FROM dim_customer_buying_group__rename_column
)

, dim_customer_buying_group__add_undefined_record AS ( 
    SELECT
      customer_buying_group_key
      , customer_buying_group_name
    FROM dim_customer_buying_group__cast_type

    UNION ALL

    SELECT
      0 AS customer_buying_group_key
      , 'Undefined' AS customer_buying_group_name

    UNION ALL

    SELECT
      -1 AS customer_buying_group_key
      , 'Invalid 'AS customer_buying_group_name
)

SELECT
  customer_buying_group_key
  , customer_buying_group_name
FROM dim_customer_buying_group__add_undefined_record