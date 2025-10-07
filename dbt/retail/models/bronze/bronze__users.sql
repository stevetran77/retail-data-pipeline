with raw as (
  select
    user_id,
    trim(full_name) as full_name,
    lower(trim(username)) as username,
    lower(trim(email)) as email,
    regexp_replace(trim(phone), r'[^0-9+]', '') as phone_normalised,
    trim(city) as city,
    created_at as created_at_raw,
    {{ parse_dirty_timestamp('created_at') }} as created_at_ts
  from {{ source('raw_csv', 'users') }}
)

select
  user_id,
  full_name,
  username,
  email,
  phone_normalised as phone,
  city,
  created_at_raw,
  created_at_ts as created_at,
  created_at_ts is null as created_at_parse_failed
from raw
