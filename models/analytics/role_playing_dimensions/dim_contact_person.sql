SELECT
  person_key AS contact_person_key
  , full_name AS contact_person_full_name
  , preferred_name AS contact_person_preferred_name
FROM {{ ref('dim_person') }}
WHERE
  is_employee = 'Employee'
  OR person_key IN (0, -1)