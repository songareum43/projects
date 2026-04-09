select
  mem_no as customer_id,
  gender,
  age,
  addr as address,
  join_date
from {{ source('main', 'car_member') }}