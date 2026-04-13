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

def _transform(**kwargs):
    # _extract에서 추출한 데이터를 XCom을 통해서 획득
    # 1. XCom을 통해서 이전 task에서 전달한 데이터 획득
    ti = kwargs['ti'] # 이게 있어야 xcom 접속 가능
    json_file_path = ti.xcom_pull(task_ids='extract')
    #로그 출력
    logging.info(f'전달받은 데이터 {json_file_path}')

    # 이 데이터를 df(pandas 사용, 소량 데이터)로 로드 
    df = pd.read_json(json_file_path)

    # 섭씨를 화씨로 일괄 처리(1번에 n개의 센서에서 데이터가 전달)
    # 설정 : 우리 공장에서는 측정 온도가 섭씨 100도 이하만 정상 데이터로 간주 -> 일단 버리는 것으로 사용
    #       100도 이상 데이터는 이상 탐지로 간주
    # 1. 100도 미만 데이터만 필터링(추출) -> pandas의 블리언 인덱싱 사용
    target_df = df[df['temperature'] < 100].copy()
    # 2. 파생 변수로 화씨 데이터 구성 (temperature_f) = (섭씨 * 9/5) + 32
    target_df['temperature_f'] = (target_df['temperature']* (9/5) + 32)


    # 3. 전처리 된 내용은 csv로 덤프 (s3로 업로드 고려)
    # 파일명 /opt/airflow/dags/data/preprocessing_data_DAG수행날짜.csv
    file_path = f'{DATA_PATH}/preprocessing_data_DAG{kwargs['ds_nodash']}.csv'
    # 저장
    target_df.to_csv(file_path, index=False) # 인덱스 제외
    logging.info(f'전저리 후 csv 저장 완료 {file_path}') # airflow가 aws에서 가동되면 s3로 저장

    # 4. csv 경로 xcom을 통해서 개시
    return file_path

# 3. DAG 정의
with DAG(
    dag_id = '06_multi_dag_2step_transform', 
    description = "transform 전용 dag", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = '@daily', # 분 시 ... => 매일 오전 09시 00분 스케줄 작동
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['etl', 'transform']
) as dag:
    task_transform = PythonOperator(
        task_id='transform',
        python_callable=_transform

    )