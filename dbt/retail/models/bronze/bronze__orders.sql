with raw as (
  select
    order_id,
    user_id,
    order_created_at as order_created_at_raw,
    {{ parse_dirty_timestamp('order_created_at') }} as order_created_at_ts,
    cast(total_amount as numeric) as total_amount,
    lower(trim(payment_method)) as payment_method,
    lower(trim(status)) as order_status,
    trim(branch) as branch
  from {{ source('raw_csv', 'orders') }}
)

select
  order_id,
  user_id,
  order_created_at_raw,
  order_created_at_ts as order_created_at,
  order_created_at_ts is null as order_created_at_parse_failed,
  total_amount,
  payment_method,
  order_status,
  branch
from raw
