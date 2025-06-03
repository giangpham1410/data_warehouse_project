WITH
  dim_city__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__cities`
)

, dim_city__rename_column AS (
    SELECT
      city_id AS city_key
      , city_name
      , state_province_id AS state_province_key
    FROM dim_city__source

)

SELECT *
FROM dim_city__rename_column