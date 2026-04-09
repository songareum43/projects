select
    prod_cd as product_id,
    brand,
    type as car_type,
    model as model_name,
    -- price가 varchar이므로 계산을 위해 숫자로 변환
    cast(replace(price, ',', '') as unsigned) as unit_price
from {{ source('main', 'car_product') }}