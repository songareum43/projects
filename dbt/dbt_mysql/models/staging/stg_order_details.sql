select
    order_no,
    prod_cd as product_id,
    quantity
from {{ source('main', 'car_orderdetail') }}