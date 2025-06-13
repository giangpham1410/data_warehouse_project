WITH
  fact_target_salesperson__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_target_salesperson__rename_column AS (
    SELECT
      year_month
      , salesperson_person_id AS salesperson_person_key
      , target_revenue
    FROM fact_target_salesperson__source
)

SELECT *
FROM fact_target_salesperson__rename_column