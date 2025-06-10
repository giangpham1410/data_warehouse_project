SELECT
  person_key
  , full_name
  , preferred_name
FROM {{ ref('dim_person') }}
WHERE
  is_employee = 'Employee'
  OR person_key IN (0, -1)