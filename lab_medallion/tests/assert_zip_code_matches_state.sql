{{ config(severity='warn') }}

-- Exercise 4: the generator pairs a state with a random ZIP code, so the two are
-- inconsistent. A ZIP code must fall in one of the 3-digit prefix ranges the USPS
-- allocates to its state. Severity is a warning: the defect is in the source data
-- and cannot be fixed in the silver layer.
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where u.zip_code is not null
  and not exists (
      select 1
      from {{ ref('states') }} s
      where s.code = u.state
        and cast(left(u.zip_code, 3) as integer) between s.prefix_min and s.prefix_max
  )
