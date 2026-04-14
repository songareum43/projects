'''
- 원격 PC에서 AWS S3에 데이터를 업로드하는 간단한 DAG
    - 액세스 키가 잘 작동하는지 체크
    - 데이터량에 따른 수행시간 체크 -> 데이터를 S3에 적제하는 방식에 대한 고민 (직접 or 서비스 이용)
- 설치(호스트 PC, 로컬 PC 상)
    - pip install apache-airflow-providers-amazon
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
# 2-1. 버킷명(iam 계정-827913617635(루트 계정 ID)-리전(ap-northeast-2)-an)
BUCKET_NAME = "de-ai-09-827913617635-ap-northeast-2-an" # 글로벌하게 고유한 이름 사용

# 2-2. 업로드할 파일명 준비
FILE_NAME = 'hello.txt'

# 2-3. 업로드할 파일의 로컬 내 위치 -> 컨테이너 기반 (airflow가 컨테이너에 존재?하기 때문...)
LOCAL_PATH = f'/opt/airflow/dags/data/{FILE_NAME}' 

def _check_s3(**kwargs):
    # S3HOOK을 통해서 실제 파일이 존재하는지 체크
    # 1. s3hook 생성
    # s3hook은 conn이나 with문 사용할 필요 x => 요청할 때만 알아서 연결하고 종료해줌
    hook = S3Hook(aws_conn_id='aws_default')
    # 2. 훅을 이용하여 모든 키(객체명, 파일명 등) 조회
    keys=hook.list_keys(bucket_name=BUCKET_NAME)
    # 3. 키 체크
    if not keys:
        raise ValueError('업로드 실패') # 현재 파일만 판단 -> 추후 버킷 내 경로를 디테일하게 세분화 필요
    # 4. 실제로 존재하면 로그 출력
    for key in keys:
        logging.info(f'키 : {key}')
    

# 3. DAG 정의
with DAG(
    dag_id = '08_aws_s3_basics', 
    description = "aws 연동, s3 업로드", 
    default_args = {
        'owner'          : 'de_2team_manager',
        'retries'        : 1, 
        'retry_delay'    : timedelta(minutes=1)
    }, 
    schedule_interval = '@daily', # 분 시 ... => 매일 오전 09시 00분 스케줄 작동
    start_date = datetime(2026,2,25),                     
    catchup = False, 
    tags = ['aws', 's3']
) as dag:

    # 4. task 정의
    # 파일 생성
    task_create_file=BashOperator(
        task_id = "create_file",
        bash_command=f'echo "hello airflow & s3" > {LOCAL_PATH}'
    )
    # s3에 업로드
    task_upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=LOCAL_PATH,  # 로컬 PC 등 원본 리소스의 위치(파일명 포함) -> 넣을 파일의 위치와 이름이 다 있어야 오류 없음
        dest_key=FILE_NAME,   # s3 특정 버킷 내에 FILE_NAME으로 저장(생성) -> dest(목적지)에 저장될 key(이름)
        dest_bucket= BUCKET_NAME, # 버킷 네임
        aws_conn_id='aws_default', # aws 접속 정보
        replace=True              # 동일 파일 있으면 덮음
        )


    task_check_s3 = PythonOperator(
        task_id="check_s3",
        python_callable=_check_s3
    )

    # 5. 의존성
    task_create_file >> task_upload_to_s3 >> task_check_s3
    
    