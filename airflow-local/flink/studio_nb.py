# 재플린에서 flink 인터프린터 상황에서 파이썬 사용시 선언문
%flink.pyflink

'''
내부적으로 준비되어 있음 -> 바로 사용 가능
setting = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env=TableEnvironment.create(setting)
'''
# st_env => 자체적으로 준비되어 있는 객체 사용(전역 변수)
# 1. 입력 테이블 정의
st_env.execute_sql('''
        CREATE TABLE stock_input(
            tiker STRING,
            price DOUBLE,  -- 정밀한 실수
            event_time TIMESTAMP(3),  -- 소수점 셋째 자리 => 밀리초 단위
            WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND  -- 늦게 들어온 데이터 5초까지 기다려주기          
        ) WITH(
            'connector'='kinesis',
            'stream'='de-ai-09-an2-kds-stock-input',
            'aws.region'='ap-northeast-2',
            'scan.stream.initpos'='LATEST', -- 작동 시점부터 데이터 읽기
            'format'='json'                        
        )
    ''')

# 2. 출력 테이블 정의
st_env.execute_sql('''
        CREATE TABLE stock_output(
            tiker STRING,
            avg_price DOUBLE,
            avg_time TIMESTAMP(3)     
        ) WITH(
            'connector'='kinesis',
            'stream'='de-ai-09-an2-kds-stock-output',
            'aws.region'='ap-northeast-2', 
            'format'= 'json'                        
        )
    ''')

# 3. 연산처리(10초 단위/티커로 데이터를 그룹화, 평균 집계) -> 출력 테이블 입력
st_env.execute_sql('''
        INSERT INTO stock_output
        SELECT ticker, 
               AVG(price) as avg_price, 
               TUMBLE_END(event_time, INTERVAL '10' SECOND) as avg_time
        FROM stock_input
        GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND), ticker
    
    ''').wait()