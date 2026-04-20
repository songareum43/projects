'''
    - 현재 상황 : database 존재(athena), s3 데이터만 있다(가정)
    - airflow를 통해서 athena 기본 연동
    - 스케줄 매일 1회 진행
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
import logging

# 2. 환경 변수
BUCKET_NAME='de-ai-09-827913617635-ap-northeast-2-an'
ATHENA_DB_NAME='de-ai-09-an2-glue-db'
SRC_NAME='s3_exam_cav' # 참고 테이블 => CTAS 과정에서는 원본 테이블이 없으면 데이터를 읽을 수 없기 때문에 꼭 필요! (데이터 자체가 있는 저장소 경로도 다 나와있음)
TARGET_TABLE = 'pass_student' 
# 메타 정보, 임시 정보 필요시 저장/삭제 공간으로 활용
S3_TARGET_LOC = f's3://{BUCKET_NAME}/athena/tbl/{TARGET_TABLE}/' # 새 데이터 파일이 저장될 곳
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'  # 쿼리한 기록 로그가 저장되는 곳
# LOcation :  폴더를 가르킴

# 3. DAG 정의
with DAG(
    dag_id = '10_aws_athena_ctas_etl', 
    description = "athena ctas 작업", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = None, 
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['aws', 's3', 'athena', 'ctas'] # ctas 조회를 바탕으로 테이블 생성
) as dag:

    # 4. task 정의
    # DAG 작동하면, s3내 특정 위치에 저장된 내용, 테이블 등을 삭제 처리 -> clean
    # 매번 가동 시 깨끗한 초기 상태 유지 전략 -> 멱등성 유지 -> 기존 데이터와 꼬이는 문제 해결

    # 임시로 사용한 s3 특정 공간 삭제 -> 클린
    t1 = S3DeleteObjectsOperator(
        task_id = "clean_s3_target", # 작업 ID
        bucket = BUCKET_NAME, # 버킷 이름
        prefix= f'athena/tbl/{TARGET_TABLE}/', # 지울 폴더 경로 지정 / 파일만 지우고 싶을 때는 keys 사용
        aws_conn_id='aws_default' # 접속 정보
    )

    # 임시로 사용한 테이블 삭제 -> 클린
    t2=AthenaOperator(
        task_id = 'drop_table',
        query = f'drop table if exists {ATHENA_DB_NAME}.{TARGET_TABLE}',
        database = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,  # 쿼리 수행 결과 로그 저장 위치
        aws_conn_id='aws_default'
    )

    # csv -> 테이블 매핑 -> 쿼리 수행 -> 결과 저장(필요시 포멧 변환)
    # 테스트 응시 결과가 90점 이상 학생만 추출하여 결과를 담는 테이블 => TARGET_TABLE
    # PARQUET : 압축 형태 지원, GZIP 등 포멧 사용, 열 기반 데이터 관리
    # 90점 이상 학생들 데이터를 추출 => PARQUET 포멧 변환 => GZIP 압축 => s3_TARGET_LOC 저장
    # 해당 소스를 TARGET_TABLE이 참조하여 => Athena를 통해 쿼리 수행 => 결과 뽑기
    query=f'''
        create table {ATHENA_DB_NAME}.{TARGET_TABLE}
        with(
            format = 'PARQUET',
            parquet_compression = 'GZIP',
            external_location = {S3_TARGET_LOC} # 쿼리 후 진짜 데이터 저장 경로 지정
        as
        select id,name,score,created_at
        from {ATHENA_DB_NAME}.{SRC_NAME}
        where score >= 90
        order by score desc
    ''' 
    t3=AthenaOperator(
        task_id = 'create_table_format_parquet',
        query=query,
        database = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC, # 로그들 저장할 경로 지정
        aws_conn_id = 'aws_default',
        do_xcom_push = True # 테이블 만들어졌나 센서 가동 조건으로 xom 활용
        # xcom을 통해서 TARGET_TABLE이 생성되었는지 체크 -> 확인 -> t4 내 기타 처리 등 활용
        )
    
    # CTAS 
    # 10분 간 최대 대기, 10초 간격 감시 => 앞 테스크가 완료되었는지 점검
    # athena 상 테이블이 완료되었는지 감시
    t4=AthenaSensor(
        task_id = 'sencor',
        # 앞 테스크 감시
        query_execution_id = "{{task_instance.xcom_pull(task_ids='create_table_format_parquet')}}", # query_execution_id : 쿼리 실행할 때마다 생기는 고유의 id 값
        poke_interval = 10, # 10초 간격
        timeout = 600, # 최대 대기 시간, 10분
        aws_conn_id = 'aws_default'
    )


    # 5. 의존성 정의
    t1 >> t2 >> t3 >> t4