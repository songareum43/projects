'''
- PythonOperator 사용
- task 간 통신 => XCom 사용(airflow의 내부 context 공간을 접근하고 액세스) => task간 상호 대화
- 통신 간 사용한 데이터의 크기는 저장 공간 고려하여 가급적 raw 데이터가 아닌 raw 데이터나 상황을 접근, 판단할 수 있는 메타 정보 정도가 적절(케이스 별 상이)
'''


# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging # 레벨별로 로그 출력 (에러, 경고, 디버깅, 정보..)

# 3-1. 콜백 함수 정의
def _extract_cb(**kwargs):
    '''
        - ETL의 Extract 담당 task의 콜백함수(실직적 작업)
        - parameters
            - kwargs : airflow가 작업 실행하기 전 정보(airflow 내부에 구성되어 있는 context(딕셔너리 구조))를 접근할 수 있는 내용)
    '''
    # 1. airflow가 주입(infection)한 airflow context 정보에서 필요한 정보 추출
    # 'ti' : <TaskInstance:...> => 현재 작동 중인 taskinstance 객체를 의미
    #        대시보드 상에서 정사각형 박스 
    ti = kwargs['ti'] 
    # 'ds' : '2026-01-01', 'ds_nodash' : '20260101'
    # 이 작업을 수행하기로 한 스케줄링된 논리적인 날짜
    execute_date = kwargs['ds']
    # 실행의 고유 ID
    run_id = kwargs['run_id']

    # 2. task 본연 업무 -> 추출한 정보를 출력(로깅 활용)
    logging.info('==Extract 작업 start ==')
    logging.info(f'작업시간 {execute_date}, 실행 ID {run_id}')
    logging.info('==Extract 작업 end ==')

    # 3. XCom 테스트를 위해 특정 데이터 반환 => Xcom에 해당 데이터 push
    # 반환 행위 => 타 task에 전달하는 행위로 활용될 수 있음
    return "Data Extract 성공"
    pass

def _transform_cb(**kwargs):
    '''
        ETL의 transform 담당
        kwargs를 이용하여 
        - airflow context 정보 획득 -> t1
            - 타 task에서 전달된 데이터 획득 -> ti.xcom_pull() 처리
    '''
    # 1. ti 객체 획득
    ti = kwargs['ti']

    # 2. task 본연의 업무 => XCom 활용
    # 특정 task가 기록한 데이터를 획득하는 기본 함수 => ti.xcom_pull
    # 여러 task의 id를 가져올 수 있음으로 ids로 표기, 여러 개 가져올 때 형태는 리스트
    data = ti.xcom_pull(task_ids='extract_task_data')

    # 3. 확인
    logging.info('==transform 작업 start ==')
    logging.info(f'결과 {data}')
    logging.info('==transform 작업 end ==')
    pass

# 2. DAG 정의
with DAG(
    dag_id = '02_basics_python', # 최소로 구성된 필수 옵션
    description = "파이썬 task 구성, 통신(XCom)", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = '@once', # 수동으로 딱 한번 수행, 주기성 X 
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['python', 'xcom', 'context']
) as dag:

    # 3. Task 정의 (PythonOperator 사용, XCom 사용)
    #   ETL을 고려하여 task 정의(간단)
    extract_task = PythonOperator(
        task_id = "extract_task_data",
        # 함수 단위(많은 작업을 하나의 단위로 구성)로 작업 구성 => 콜백 함수 형태
        python_callable = _extract_cb
    )
    transform_task = PythonOperator(
        task_id = "transform_task_data",
        python_callable = _transform_cb
    )
    
    # 4. 의존성 정의
    extract_task >> transform_task
    pass