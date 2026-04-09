select
    order_no,
    mem_no as customer_id,
    order_date,
    store_cd
from {{ source('main', 'car_order') }}