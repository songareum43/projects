'''
- ma에서 silver 단계 처리
- sql을 통해 데이터(flatten, 파생변수, 컬럼명 변경) 전처리 수행
    - event_id
    - event_time => event_time_timestamp
    - data.user_id
    - data.item_id
    - data.price
    - data.qty 
    - (data.price*data.qty) as total_price
    - data.store_id
    - source_ip
    - user_agent
    - dt(year-month-day)
    - hour as hr
'''
