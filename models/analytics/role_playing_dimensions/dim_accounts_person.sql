SELECT
  person_key AS accounts_person_key
  , full_name AS accounts_person_full_name
FROM {{ ref('dim_person') }}
WHERE is_employee = 'Not Employee'