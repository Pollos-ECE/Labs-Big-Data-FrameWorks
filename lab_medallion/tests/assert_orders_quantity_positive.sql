-- Exercise 2: quantity must be strictly positive.
-- A singular test is used because dbt ships no generic test for an arbitrary
-- boolean expression; accepted_values only checks a closed list of values.
select
    order_id,
    quantity
from {{ ref('stg_orders') }}
where quantity is null or quantity <= 0
