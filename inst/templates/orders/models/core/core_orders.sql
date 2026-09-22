select order_id, customer_id, amount as order_amount,
  amount > 0 as is_positive_order
from {{ ref('stg_orders') }}
