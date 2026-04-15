'''
- Amazon Data Firehose(ADF)에게 direct로 데이터를 put 샘플
'''
# apache-airflow-providers-amazon 설치하여 자동으로 awd sdk(boto3)가 자동 설치됨
import boto3
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
        session = boto3.Session(
            aws_access_key_id = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY,
            region_name = REGION
        )
        return session.client('firehose')

    # AWS 내부에서 진행
    return boto3.client('firehose', region_name = REGION)

firehose = get_client()
print(firehose)