'''
- Amazon Data Firehose(ADF)에게 direct로 데이터를 put 샘플
'''
# apache-airflow-providers-amazon 설치하여 자동으로 awd sdk(boto3)가 자동 설치됨
import boto3 # python으로 aws를 조정할 수 있는 모듈
import json
import time

# 2. 환경변수
ACCESS_KEY = ''
SECRET_KEY = ''
REGION = 'ap-northeast-2'

# 3. 특정 서비스(ADF) 클라이언트 생성
# AWS 외부에서 진행
def get_client(service_name='firehose', is_in_aws=True):
    if not is_in_aws:
        session = boto3.Session(             # session : 환경 설정?! => 정보 담기
            aws_access_key_id = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY,
            region_name = REGION
        )
        return session.client('firehose') # client : 전용 제어판?! => 명령 내릴 프로그램 선택

    # AWS 내부에서 진행
    return boto3.client('firehose', region_name = REGION)

firehose = get_client()
print(firehose)

# 4. 로그 생성 및 ADF 발송
from run import make_one_log

# 5. 로그 1개 생성 -> adf 발송 함수
def send_log():
    # 5-1. 로그 1개 생성
    response = firehose.put_record(    # put_recode : 데이터를 한 개씩 넣기
        # 어디로? => firehose 스트림
        DeliveryStreamName = "de-ai-09-an2-kdf-log-to-s3",
        # 데이터
        Record = {
            'Data':make_one_log() + "\n" # 로그 데이터를 한줄씩 적제
        }
    )
    print(f'전송결과 : {response}') # 응답코드 -> 200 ok
    
# 6. 10번 로그 생성 발송
for i in range(10):
    send_log()