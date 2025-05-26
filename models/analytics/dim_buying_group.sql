WITH
  dim_buying_group__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

SELECT *
FROM dim_buying_group__source