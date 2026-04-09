# 1. 모듈 가져오기
import urllib.request
import json
import html
import re
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# 2. 환경변수 로드 -> .env 생성, dotenv로 불러온다 전략수립
load_dotenv()
NAVER_CLIENT_ID     = os.getenv('NAVER_CLIENT_ID')
NAVER_CLIENT_SECRET = os.getenv('NAVER_CLIENT_SECRET')
NAVER_URL           = os.getenv('NAVER_URL')
PROTOCAL            = os.getenv('PROTOCAL')
USER                = os.getenv('USER')
PW                  = os.getenv('PW')
HOST                = os.getenv('HOST')
PORT                = os.getenv('PORT') # 정수타입이 맞지만 연결문자열에서는 관계없음, 처리X
DB_NAME             = os.getenv('DB_NAME')
TABLE_NAME          = os.getenv('TABLE_NAME')

# 3. Extract

# 1. 함수화(모듈화)
def get_naver_url( keyword : str ) -> str:
  # '주소' + '?' + 'query' + '=' + keyword
  return f'{NAVER_URL}?query={ urllib.parse.quote(keyword) }'

def get_news(keyword:str) -> list:
  '''
    키워드 -> .... -> [ {}, {}, .. {} ]
  '''  
  print( get_naver_url(keyword) )
  req = urllib.request.Request( get_naver_url(keyword) )
  req.add_header('X-Naver-Client-Id', NAVER_CLIENT_ID)
  req.add_header('X-Naver-Client-Secret', NAVER_CLIENT_SECRET)
  res = urllib.request.urlopen( req )
  res_code = res.getcode()
  if res_code == 200:
    return json.load( res )[ 'items' ] # list : 뉴스 10개
  else:
    print('통신 실패', res_code)
    return [] # {}

news = get_news('유가')

# 4. Transform
pattern=re.compile("<[a-z0-9]+>|</[a-z0-9]+>", re.IGNORECASE)

def data_clean( src: str) -> str:
  '''
    - 결측치, 이상치 처리
    - 노이즈 처리 (제거 or 대체)
    - ...
  '''
  # 1. &quot;등 표현 대체(원래 문자로)
  src = html.unescape( src ) # &quot; => "
  # 2. 정규식을 통해서 html 태그 제거
  src = pattern.sub("", src) # HTML 태그가 발견되면 ""로 대체
  # 3. 좌우 공백 제거
  return src.strip()

# 필요한 데이터만 추출/정제 => 전처리함
today_news = [
  {
    # 실제값으로 채우기 -> 실습
    'title'       : data_clean(item['title']),
    'description' : data_clean(item['description']),
    'pubDate'     : item['pubDate']
  }
  for item in news#[:1] # 1개만 테스트
]

# 5. Load
df     = pd.DataFrame.from_dict( today_news )

db_url = f'{PROTOCAL}://{USER}:{PW}@{HOST}:{PORT}/{DB_NAME}'
print( db_url )

# 3. 엔진 생성
engine = create_engine( db_url )
# 4. 실제 접속
conn = engine.connect()
# 5. 데이터 적제
#        (테이블명, 연결정보, 기존데이터가존재하면 추가, 인덱스데이터 제거)
df.to_sql( name=TABLE_NAME, con=conn, if_exists='append', index=False)
# 6. 연결종료(접속 해제, 커넥션 반납)
conn.close()