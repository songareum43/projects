'''
- ma에서 silver 단계 처리
- 스케줄 (10 * * * *)
    - firehose에서 버퍼 시간을 최대 3분으로 구성 -> 3분 이후부터는 스케줄 가동 가능
    - 보수적으로 10분에 작업하도록 구성
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
- 작업(silver 테이블 삭제 -> ctas)
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# 2. 환경변수
DATABASE_BRONZE = 'de-ai-09-ma-bronze-db'
DATABASE_SILVER = 'de-ai-09-ma-silver-db'
SILVER_S3_PATH = 's3://de-ai-09-827913617635-ap-northeast-2-an/medallion/silver/'
ATHENA_RESULTS = 's3://de-ai-09-827913617635-ap-northeast-2-an/athena-results/'

# 3. DAG 정의

    # 4. TASK 정의

    # 5. 의존성