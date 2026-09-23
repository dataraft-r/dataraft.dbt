select customer_id, sum(order_amount) as revenue
from {{ ref('core_orders') }}
group by customer_id
