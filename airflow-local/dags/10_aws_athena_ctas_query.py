'''
- DAG 스케줄은 하루에 한번(00시 00분 00초) 지정 -> 테스트는 트리거 작동
- T1 : s3에 특정 위치에 적제된 데이터를 기반으로 테이블 구성
  - cavs/ 하위 데이터를 기반으로 테이블 구성 -> s3_exam_csv_tbl
- T2 : 해당 테이블을 이용하여 분석 결과를 담은 테이블 삭제(존재하면)
  - daily_report_tbl 삭제 쿼리 수행(존재할 경우)
- T3 : T1에서 만들어진 테이블을 기반으로 분석 결과를 도출하여 분석 결과를 담는 테이블에 연결 -> 결과 레포트용 데이터
  - 시험 결과를 기반으로 결과, 카운트, 평균, 최소, 최대 -> 그룹화 수행(기준 result)
  - 테이블명 => daily_report_tbl
    - format = 'PARQUET'
    - external_location = '원하는 s3 위치로 지정' -> 쿼리 결과 저장
  - output_location = '원하는 s3 위치로 지정' -> 테이블 메타 정보 저장
- 미구현 -> T3 데이터를 기반으로 대시보드 구성 -> 원하는 시간에 결과 파악
- 의존성 : T1 >> T2 >> T3 
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
import logging

# 2. 환경 변수
BUCKET_NAME='de-ai-09-827913617635-ap-northeast-2-an'
ATHENA_DB_NAME='de-ai-09-an2-glue-db'
CSV_TABLE='s3_exam_csv_tbl'
TARGET_TABLE='daily_report_tbl'

# 메타 정보, 임시 정보 필요시 저장/삭제 공간으로 활용
S3_TARGET_LOC = f's3://{BUCKET_NAME}/athena/tbl/query' 
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'  

# 3. DAG 정의
with DAG(
    dag_id = '10_aws_athena_ctas_query', 
    description = "athena query 작업", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = '@daily', 
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['aws', 's3', 'athena', 'query'] # ctas 조회를 바탕으로 테이블 생성
) as dag:
    
    # 4. task 정의
    query1=f'''
        create EXTERNAL table if not exists {CSV_TABLE}(
        id int,
        name string,
        score int,
        created_at string,
        result string
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 's3://de-ai-09-827913617635-ap-northeast-2-an/csvs/'
    TBLPROPERTIES ("skip.header.line.count"="1"); 
    ''' 
    # EXTERNAL 이거 빼먹고 생성하니 오류 발생

    t1 = AthenaOperator(
        task_id='create_table',
        query=query1,
        database = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,  
        aws_conn_id='aws_default'
    )

    t2 = AthenaOperator(
        task_id='delete_table',
        query = f'drop table if exists {TARGET_TABLE}',
        database = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,  
        aws_conn_id='aws_default'
    )

    query=f'''
        create table {TARGET_TABLE}
        with(
            format = 'PARQUET',
            parquet_compression = 'GZIP',
            external_location = '{S3_TARGET_LOC}'
        ) 
        as
        select result, count(result) as count, avg(score) as avg, min(score) as min, max(score) as max
        from {CSV_TABLE}
        group by result;
    ''' 

    t3 = AthenaOperator(
        task_id='result_table',
        query = query,
        database = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,  
        aws_conn_id='aws_default'
    )

    # 새 테이블 생성할 때는 반드시 별칭 부여 필요...!!

    # 5. 의존성
    t1 >> t2 >> t3
   
    pass
