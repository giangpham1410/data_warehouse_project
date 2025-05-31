WITH dim_date__generate AS (
  SELECT
    *
  FROM UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-12-31', INTERVAL 1 DAY)) AS date
)

, dim_date__enrich AS (
    SELECT
      date
      , EXTRACT(YEAR FROM date) AS year_number
      , DATE_TRUNC(date, YEAR) AS year -- first day in the year
      , DATE_TRUNC(date, MONTH) AS year_month -- first day in the month
      , FORMAT_DATE('%B', date) as month
      , FORMAT_DATE('%A', date) AS day_of_week
      , FORMAT_DATE('%a', date) AS day_of_week_short
      --, FORMAT_DATE('%Q', d) as quarter
      --, EXTRACT(WEEK FROM d) AS year_week
      --, EXTRACT(DAY FROM d) AS year_day
      ----, EXTRACT(YEAR FROM d) AS fiscal_year
      --, EXTRACT(MONTH FROM d) AS month
      --, FORMAT_DATE('%b', d) as month_name
      --, FORMAT_DATE('%w', d) AS week_day
    FROM dim_date__generate
)

SELECT
  *
  , CASE
      WHEN day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday' 
      WHEN day_of_week_short IN ('Sat', 'Sun') THEN 'Weekend' 
      ELSE 'Invalid'
      END AS is_weekday_or_weekend
FROM dim_date__enrich


