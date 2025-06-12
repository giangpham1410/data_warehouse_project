SELECT *
FROM {{ ref('dim_person') }}
WHERE is_employee <> 'Employee'