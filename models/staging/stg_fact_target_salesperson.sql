WITH
  fact_target_salesperson__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_target_salesperson__rename_column AS (
    SELECT
      year_month
      , salesperson_person_id AS salesperson_person_key
      , target_revenue AS target_gross_amount
    FROM fact_target_salesperson__source
)

, fact_target_salesperson__cast_type AS (
    SELECT
      DATE_TRUNC(
        CAST(year_month AS DATE), MONTH -- Vd user nhap 2025-12-15 thi se auto tra ve 2025-12-01
       ) AS year_month
      , CAST(salesperson_person_key AS INTEGER) AS salesperson_person_key
      , CAST(target_gross_amount AS NUMERIC) AS target_gross_amount
    FROM fact_target_salesperson__rename_column
)

SELECT
  year_month
  , salesperson_person_key
  , target_gross_amount
FROM fact_target_salesperson__cast_type