# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import random
import pandas as pd
import os 

# 2. 환경변수 
DATA_PATH='/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

def _extract(**kwargs):
    # 스마트팩토리에 설치된 오븐 온도 센서 데이터가 발생되면 데이터레이크(s3, 어딘가에 존재)에 쌓이고 있음
    # -> 추출해서 가져오는 단계로 가정

    # 더미 데이터 고려 구성 -> 1회성으로 10건 구성 -> [ {}, {} ...]
    data = [
        {
            "sensor_id" : f"SENSOR_{i+1}", # 장비 ID
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # YYYY-MM-DD hh:mm:ss
            "temperature" : round( random.uniform(20.0, 150.0), 2),
            "status" : "on" # 'off'
        } for i in range(10) ]

    # 더미 데이터를 파일로 저장 (로그 파일처럼) -> json 형태
    # /opt/airflow/dags/data/sensor_data_DAG수행날짜.json
    file_path = f'{DATA_PATH}/sensor_data_{kwargs["ds_nodash"]}.json'
    with open(file_path, 'w') as f:
        json.dump(data,f) # data를 json 형태로 변환하여 담기

    # 로그는 별도의 프로그램에서 지속적으로 발생
    # 현재는 편의상 airflow에 포함시킴

    #XCom을 통해서 task_transform에게 전달 (로그의 경로를 전달)
    logging.info(f'extract 한 로그 데이터 {file_path}')
    return file_path

with DAG(
    dag_id = '06_multi_dag_1step_extract', 
    description = "extract 적용 dag", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = '@daily', # 분 시 ... => 매일 오전 09시 00분 스케줄 작동
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['etl', 'extract']
) as dag:
     task_extract = PythonOperator(
        task_id='extract',
        python_callable=_extract
    )