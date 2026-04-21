'''
- 멀티 프로세스로 데이터 발생
  - store-01 ~ store-(프로세스수) : 점포별로 데이터 발생 구성
- 실행
  airflow-local> python ./log_gen/multi_process_data_gen.py
'''
# 1. 모듈 가져오기
import json
import os
import uuid
import random
import boto3
import time
from datetime import datetime, UTC
from faker import Faker
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import multiprocessing  

load_dotenv()
# 2. 페이커 생성
fake = Faker()
AWS_ACCESS_KEY=os.getenv('ACCESS_KEY')
AWS_SECRET_KEY=os.getenv('SECRET_KEY')
AWS_REGION = 'ap-northeast-2'
KINESIS_DATA_STREAM_NAME = 'de-ai-09-ap2-kdf-medallion-bronze-stream'

# 3. AWS 연동 => session 설정 => I/O
try:
  # kinesis 세션(클라이언트) 획득
  session = boto3.Session(
      aws_access_key_id = AWS_ACCESS_KEY,
      aws_secret_access_key = AWS_SECRET_KEY,
      region_name = AWS_REGION
  )
  kinesis_client = session.client('kinesis')
  print('AWS 연동 성공')
except Exception as e:
  print('AWS 연동 실패')

def gen_data(store_id):
  '''
    데이터 더미 생성
  '''
  # 제품 정보(마스터)
  items = [
      {"item_id": "bread-001", "item_name": "우유식빵",             "price": 5500},
      {"item_id": "bread-002", "item_name": "천연발효버터치아바타", "price": 4800},
      {"item_id": "coffee-01", "item_name": "아메리카노",           "price": 4000},
      {"item_id": "jam-01",     "item_name": "수제 딸기잼",         "price": 8500}
  ]
  # 제품 1개 선택
  selected_item = random.choice(items)
  # 수량 램덤 구성
  qty = random.randint(1, 3)
  # 현재 시간 세팅
  current_utc_time = datetime.now(UTC).isoformat().replace("+00:00", "Z")

  # raw 데이터 구성
  raw_log = {
      "event_id"  : str(uuid.uuid4()),  # 이벤트 관리 번호(중복되지 않게 해싱)
      "event_time": current_utc_time,   # 현재시간(이벤트 발생시간)
      "source_ip" : fake.ipv4(),        # 클라이언트의 접속 IP(페이크)
      "user_agent": fake.user_agent(),  # 클라이언트의 접속 브라우저 타입(페이크)
      "data": json.dumps({              # 상세 데이터 => dict내에 dict 구성, 객체 직렬화추가
          "user_id" : f"user_{random.randint(100, 999)}", # 사용자 id (사용자별로 여러번구매)
          "item_id" : selected_item["item_id"],           # 구매 제품
          "price"   : selected_item["price"],             # 단가
          "qty"     : qty,                                # 수량
          "store_id": store_id                          # 매장번호(오프/온라인 포함) 
      }),
      "ingested_at": current_utc_time   # 로그 발생 시간(event_time) 동일
  }
  return raw_log

def send_to_kinesis(log_entry):
  '''
    kinesis로 데이터 전송
  '''
  try:
    kinesis_client.put_record(
      StreamName = KINESIS_DATA_STREAM_NAME,
      Data = json.dumps(log_entry),
      PartitionKey = log_entry['event_id']
    )
    return True
  except Exception as e:
    print('AWS 전송 에러', e)
    return False

def run_producer(i, store_id):
  try:
    print(f'프로세스-{i} 가동')
    while True:
      log_entry = gen_data(store_id)
      if send_to_kinesis(log_entry):
        print(f"{log_entry['event_time']} 전송 성공 {store_id}")
      time.sleep( random.uniform(0.5, 1.5) )
      
  except KeyboardInterrupt:
    print('발생 중단')
  print(f'프로세스-{i} 종료')

if __name__ == '__main__':
    # 동시에 진행할 프로세스의 수
    NUM_STORES = 4
    processes = list() # 프로세스 보관용 -> 조인
    # 로그 출력
    print(f'{NUM_STORES}개의 프로세스(점포)가 데이터를 발생시켜서 kinesis에 전송')
    for i in range(NUM_STORES): # 4번 반복
      # 점포(매장) id 생성
      store_id = f"store-{str(i+1).zfill(2)}" # 01, 02, 03
      # 멀티프로세서를 이용하여 프로세스 생성
      p = multiprocessing.Process(target=run_producer, args=(i, store_id)) # Process : 새로운 방 만들기 설계도 (실행 시킬 함수, 필요한 매개 변수)
      processes.append(p)
      # 프로세스 가동 => 함수 호출 => 내부에서 무한 루프 => 데이터 생성 및 전송
      p.start()
      # 종료 처리
      try:
        for p in processes:
          p.join # 자식 프로세스가 모두 끝날 때까지 대기
      except Exception as e: # ctrl + c
        print('종료')


