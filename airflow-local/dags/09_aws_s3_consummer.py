'''
- 평시 -> 잠복하듯 센서 켜고 대기
- 특정 버킷 혹은 버킷 내 공간을 감시(sensor) -> 파일(객체 등) 업로드 -> 감지 -> DAG 작동
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # s3 키 읽는 용도
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # 감시용 센서
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 특정 데이터(객체) 삭제
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