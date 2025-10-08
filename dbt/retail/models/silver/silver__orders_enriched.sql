with orders as (
  select *
  from {{ ref('bronze__orders') }}
),
users as (
  select *
  from {{ ref('bronze__users') }}
),
joined as (
  select
    o.order_id,
    o.user_id,
    o.order_created_at,
    o.order_created_at_parse_failed,
    date(o.order_created_at) as order_date,
    o.total_amount,
    o.payment_method,
    o.order_status,
    o.branch,
    u.full_name,
    u.email,
    u.phone,
    u.city as user_city,
    u.created_at as user_created_at,
    u.created_at_parse_failed as user_created_at_parse_failed
  from orders o
  left join users u using (user_id)
)

select *
from joined
