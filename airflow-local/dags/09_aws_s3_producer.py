'''
- 데이터 생산(ETL 등을 통해서) -> csv -> s3 업로드(push) 처리
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging


# 2. 환경변수 설정

    

# 3. DAG 정의
with DAG(
    dag_id = '09_aws_s3_producer', 
    description = "aws 연동, s3 업로드", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = '@daily', # 분 시 ... => 매일 오전 09시 00분 스케줄 작동
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['aws', 's3', 'producer']
) as dag:

    # 4. task 정의


    # 5. 의존성

    pass