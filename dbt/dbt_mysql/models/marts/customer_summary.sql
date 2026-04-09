위에서 가상 테이블 정의, select에서 테이블처럼 사용하는 문법 지원 => with ~ as
with 
customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    -- 주문 정보와 상품 가격을 결합하여 총액 계산
    select
        o.customer_id,
        count(distinct o.order_no) as total_order_count,
        sum(d.quantity * p.unit_price) as total_spent,
        max(o.order_date) as last_order_date
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_order_details') }} d on o.order_no = d.order_no
    join {{ ref('stg_products') }} p on d.product_id = p.product_id
    group by 1
)
select
    c.*,
    -- 여러 개의 인자 중에서 NULL이 아닌 첫 번째 값을 반환
    coalesce(o.total_order_count, 0) as total_order_count,
    coalesce(o.total_spent, 0) as total_spent,
    datediff(current_date(), o.last_order_date) as recency_days,
    -- 고객 등급 분류 로직
    -- 1억, 5천만원
    case
        when o.total_spent >= 100000000 then 'VIP'
        when o.total_spent >= 50000000 then 'Gold'
        else 'Silver'
    end as customer_tier
from customers c
left join orders o on c.customer_id = o.customer_id
