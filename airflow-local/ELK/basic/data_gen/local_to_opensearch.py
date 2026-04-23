'''
- 로그(데이터) 발생 -> opensearch -> 직접 전송
'''
# 1. 모듈 가져오기
from opensearchpy import OpenSearch
from datetime import datetime
import random
import time

# 2. 환경변수
from dotenv import load_dotenv
import os

load_dotenv()

HOST = os.getenv('OPENSEARCH_HOST')
AUTH = (os.getenv('AUTH_NAME'), os.getenv('AUTH_PW'))


# 3. AWS Opensearch 서비스 클라이언트 연결
client = OpenSearch(
    hosts = [{"host": HOST, "port" : 443}], # https -> 443, 오픈서치 주소
    http_auth = AUTH, # 비밀번호를 통한 인증?!
    http_compress = True, # 압축해서 올리기
    use_ssl = True, # 보안 통로
    verify_certs  = True, # 보안 통로
    ssl_assert_hostname = False, # 이름 조금 달라도 들여보내기? (정확한 이름 확인 x)
    ssl_show_warn = False  # 경고창 띄우지 않기
)

# 4. 인덱스 생성 => (스마트 팩토리 -> 센서(장비)별로 구성/)
# 인증 오류(401) 발생하면(1회성 조치) => 오픈서치 대시보드 URL 접속
index_name = 'factory-45-senser-v1' # 공장 내 45구역에 v1 센서 의미 부여(설정)
if not client.indices.exists(index=index_name): # 인덱스가 없는가?
    client.indices.create(index=index_name)   # 인덱스 생성

# 5. 데이터 생성 => 전송
# 공장에 장비(오븐) 3대, 데이터를 1초마다 전송
oven_ids = ['OVEN_001', 'OVEN_002', 'OVEN_003']
while True:
    for oven_id in oven_ids:
        # 온도 생성
        temp = random.uniform(200,220) # 정상범위
        if random.random() > 0.95: # 0.0 ~ 1.0 사이 값 중 5% 확률로 -> 이상 온도 발생
            temp+= random.uniform(30,50) # 임의로 온도 증가 -> 이상 데이터 구성
        # json(반정형) 형태의 데이터 구성
        doc = {
            'timestamp':datetime.now(),
            'oven_id':oven_id,
            'temperature':round(temp,2), # 온도
            'vibration':round(random.uniform(0, 0.15), 2),  # 진동
            'status':'DANGER' if temp >= 240 else 'NORMAL' # 실제 raw data에서는 없을 수 있음
        }

        # 전송( https 방식, post 전송으로 예상 )
        response = client.index(
            index=index_name,
            body=doc,
            refresh=True
        )
        print(f"{oven_id} 온도 : {doc['temperature']} 전송 완료")
    # 오븐 값 3개 전송 후 1초 대기
    time.sleep(2)