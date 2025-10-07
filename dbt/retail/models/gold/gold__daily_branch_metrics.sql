with fact_orders as (
  select *
  from {{ ref('silver__orders_enriched') }}
  where order_created_at is not null
),
aggregated as (
  select
    order_date,
    branch,
    count(distinct order_id) as orders_count,
    count(distinct user_id) as customers_count,
    sum(case when order_status = 'completed' then total_amount else 0 end) as completed_revenue,
    sum(case when order_status != 'completed' then total_amount else 0 end) as non_completed_revenue,
    sum(total_amount) as gross_revenue
  from fact_orders
  group by order_date, branch
)

select *
from aggregated
