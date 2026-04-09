'''
- etl 간단하게 적용, 스마트팩토리 상 온도 센서에 대한 ETL 처리, mysql 사용
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
# 추가분
# from airflow.providers.mysql.operators.mysql import MysqlOperator
# Load 처리 시 sql에 전처리된 데이터를 밀어 넣을 때 사용
from airflow.providers.mysql.hooks.mysql import MySqlHook
# 데이터
import json
import random
import pandas as pd # 소량의 데이터(데이터 규모)
import os

# 2. 환경변수 
# 프로젝트 내부 폴더를 데이터용으로 (~/dags/data) 지정
# task 진행 간 생성되는 파일을 동기화하도록 위치 지정 -> 향후 s3(데이터 레이크)로 대체 될 수 있음
# 도커 내부에 생성된 컨테이너 상 워커 내 airflow 상 저장한 데이터 위치
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
            "status" : "on", # 'off'
        } for i in range(10) ]

    # 더미 데이터를 파일로 저장 (로그 파일처럼) -> json 형태
    # /opt/airflow/dags/data/sensor_data_DAG수행날짜.json
    file_path = f'{DATA_PATH}/sensor_data_{kwargs["ds_nodash"]}.json'
    with open(file_path, 'w') as f:
        json.dump(data,f)

    # 로그는 별도의 프로그램에서 지속적으로 발생
    # 현재는 편의상 airflow에 포함시킴

    #XCom을 통해서 task_transform에게 전달 (로그의 경로를 전달)
    logging.info(f'extract 한 로그 데이터 {file_path}')
    return file_path
    pass

def _transform(**kwargs):
    # _extract에서 추출한 데이터를 XCom을 통해서 획득
    # 1. XCom을 통해서 이전 task에서 전달한 데이터 획득
    ti = kwargs['ti']
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
    pass

def _load(**kwargs):
    # csv => df => mysql 적제
    pass

# 3. DAG 정의
with DAG(
    dag_id = '05_mysql_etl', 
    description = "etl 수행하여 mysql에 온도 센서 데이터 적제", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = '@daily', # 분 시 ... => 매일 오전 09시 00분 스케줄 작동
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['etl', 'mysql']
) as dag:

    # 4. task 정의
    #task_create_table = MysqlOperator(
        # table 생성, if not exists를 사용하여 무조건 sql이 일단 수행되게 구성 -> 안하면 fail 발생(2회차부터)
        # 최초는 생성, 존재하면 pass => if not exists
        # task_id="create_table",
        # 연결 정보
       # mysql_conn_id="mysql_default", #  대시보드 admin > connections> 하위에 사전에 등록
        # sql
        # sql = '''
           # CREATE TABLE IF NOT EXISTS sensor_readings (
               # id INT AUTO_INCREMENT PRIMARY KEY,
                #sensor_id VARCHAR(50),
                #timestamp DATETIME,
                #temperature_c FLOAT,
                #temperature_f FLOAT,
                #created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            #);
        #''' 
   # ) 
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=_extract
    )
    task_transform = PythonOperator(
        task_id='transform',
        python_callable=_transform

    )
    task_load = PythonOperator(
        task_id='load',
        python_callable=_load
    )
    # 5. 의존성 정의
    # task_create_table >> 
    task_extract >> task_transform >> task_load
    pass