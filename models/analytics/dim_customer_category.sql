WITH
  dim_customer_category__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customer_categories`
)
SELECT *
FROM dim_customer_category__source