# 기본 골격 준비

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
import logging

# 2. 환경변수 설정

    

# 3. DAG 정의
with DAG(
    dag_id = '09_aws_s3_consummer', 
    description = "aws 연동, s3 업로드", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = '@daily', # 분 시 ... => 매일 오전 09시 00분 스케줄 작동
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['aws', 's3', 'consummer']
) as dag:

    # 4. task 정의


    # 5. 의존성

    pass