'''
- 평시 -> 잠복하듯 센서 켜고 대기
- 특정 버킷 혹은 버킷 내 공간을 감시(sensor) -> 파일(객체 등) 업로드 -> 감지 -> DAG 작동
- 렌트카 => 개인 촬영 => 업로드 => s3 => 트리거 작동 => 데이터 추출 전처리 => AI 모델 전달 => 추론 => 평가 => 피해액 응답
    - 사용자가 언제 사진을 올릴지 아무도 모름 -> 의외성 -> 소카 모델 확인
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # s3 키 읽는 용도
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # 감시용 센서
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 특정 데이터(객체) 삭제
import logging

# 2. 환경변수 설정 -> 특정 버킷 내에 특정 키에 변화가 왔는지만 궁금함 -> 필요한 정보만 세팅
BUCKET_NAME = "de-ai-09-827913617635-ap-northeast-2-an"
FILE_NAME = 'sensor_data.csv'
S3_KEY=f'income/{FILE_NAME}' 
    

# 4-1. 콜백 함수
def _reading_data(**kwargs):
    # s3 훅 사용
    hook = S3Hook(aws_conn_id="aws_default")
    # 읽기(비지니스)
    data = hook.read_key(key=S3_KEY, bucket_name=BUCKET_NAME)
    # 로그 출력
    logging.info('--로그 출력 시작--')
    logging.info(data)
    logging.info('--로그 출력 시작--')

# 3. DAG 정의
with DAG(
    dag_id = '09_aws_s3_consummer', 
    description = "s3 특정 버킷(사용자 별로 섹션 할당/이름 구분 등)에 대해 데이터 변화를 감지 -> 읽기 -> 비지니스 처리 -> 삭제(특정 위치에 보관(raw data 구축))", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    # 최소 스케줄은 필요함 -> None x
    schedule_interval = '@daily', # DAG는 활성화 정도만 구성, 센서 작동에 스케줄이 필요한지 확인
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['aws', 's3', 'consummer']
) as dag:

    # 4. task 정의

    # 감시자(센서, 옵저버)
    task_waitting_trigger = S3KeySensor(
        task_id = "task_waitting_trigger",
        # 감시 대상 설정
        bucket_key= S3_KEY, # 버킷 내 타겟
        bucket_name=BUCKET_NAME, # 버킷 이름
        aws_conn_id= 'aws_default', # 접속 정보
        # 감시 방법
        mode = 'reschedule', # 대기 중에 자원 반납
        poke_interval = 10, # 10초 간격으로 체크(주기에 따라 자원 사용 차이 발생)
        timeout = 60*10, # 서비스 가동 후(스케줄에 의해) 10분 넘게 감지가 안되면 종료
    )

    # 뭔가 작업(비지니스)
    task_reading_data = PythonOperator(
        task_id = "task_reading_data",
        python_callable=_reading_data
    )

    # 파일(키) 삭제 / 필요시 특정 위치 보관 -> 뒷처리
    task_delete_data_or_backup = S3DeleteObjectsOperator(
        task_id = "task_delete_data_or_backup",
        bucket=BUCKET_NAME,
        keys= [S3_KEY], # 삭제 대상, n개 지정 가능
        aws_conn_id= 'aws_default'
    )


    # 5. 의존성
    # 센서 감지 -> 작업 -> 키 제거 -> 특정 위치는 최초 상태로 돌아간다
    task_waitting_trigger >> task_reading_data >> task_delete_data_or_backup
    pass