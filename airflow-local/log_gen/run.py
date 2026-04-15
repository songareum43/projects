'''
- 로그 생성기 사용 예시
'''

import json
import time
# 현재 워킹디렉토리에서 코드를 작동할 때 경로
from log_generator import LogGenerator

log_gen = LogGenerator() 

def make_log():
  print(f'finance 로그 생성 시작')
  print('-'*50)
  for i in range(50):
    log = log_gen.finance()
    # ensure_ascii 이스케이프 문자들 변환없이 있는 그대로 출력하는가?
    log_json = json.dumps(log, ensure_ascii=False)
    print(f'[Log-{i+1}]{log_json}')
    # 다음 로그 발생까지 대기시간, 동시간에 n개 로그 x
    # 다른 시간대에 1개의 로그 형태임 -> 컨셉에 따라 다르게 구성 가능
    time.sleep(log_gen.get_interval_time('random', 1))
    # break
  print('-'*50)

if __name__=="__main__":
    make_log()
