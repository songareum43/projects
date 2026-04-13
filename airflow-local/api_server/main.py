'''
- 요구사항
    - 신용평가(예측)만 담당하는 API 구성
    - AI 모델이 처리하는 것처럼 전체 틀 구성, 실제로는 간단한 공식 처리
    - API
        - 요청 데이터
            - 1 ~ n명 데이터 => [개별 정보(dict),...]
        - 응답 데이터
             - 1 ~ n명 데이터 => [개별 평가 정보(dict)...]

- 로컬 PC
    - 패키지 설치(필요시 가상환경에서 수행)
        - pip install fastapi pydantic uvicorn
'''
# 1. 모듈 가져오기
from fastapi import FastAPI    # 앱 자체 의미
from pydantic import BaseModel # 해당 클래스를 상속 -> 요청/응답 데이터 구조 정의
from typing import List        # 요청 데이터에 대한 형태 정의(n개 데이터 타입 표현) -> 유효성 점검용
import random                  # 신용평가 시 활용

# 2. fastapi app(객체) 생성
# 해당 변수명은 uvicorn에서 구동시 `모듈명:FastAPI 객체명`에서 객체명에 해당 
app = FastAPI()

# 3. 요청/응답 데이터 구조를 정의 + 유효성 검사 틀을 제공하는 클래스 구성 -> pydantic 사용
# BaseModel을 상속 -> pydantic 사용 가능 -> 틀/구조 정의
# 클래스 구성원들 중 클래스 멤버 -> 키 값으로 활용 -> 타입 부여 -> 왜? 유효성 검사를 위해
class ReqData(BaseModel): # 요청
    # 사용자 아이디, 소득, 대출 총량
    user_id:str # 타입 힌트
    income:int
    loan_amt:int
    pass

class ResData(BaseModel): # 응답
    # 사용자 아이디, 신용 접수, 등급
    user_id:str
    credit_score:int # 0점 ~ 1000점
    grade:str # A, B, C 등급
    pass

# 4. 라우팅(url 정의, 해당 요청 시 처리할 함수 매칭)  
# @app => 데코레이터 => 함수 안에 함수가 존재하는 2중 구조 => 특정 함수에 공통 기능 부여시 유용
# 웹프로그램에서 자주 보임(요청을 전달하는 기능 공통 등...)
# '/' => 홈페이지 주소 표현
@app.get('/') # URL 정의, http 프로토컬의 method 정의(get 방식)
def home():
    return {'status':'AI 신용평가 서비스 API'}

# 신용평가 api 서비스
# post 전송 사유 : 보안, 대량 데이터, http body를 통해서 전달 등...
# response_model : 응답 데이터는 이런 형태로 보내라는 의미 -> 유효성 검사 자동 수행
@app.post("/predict", response_model=List[ResData])
def predict(users:List[ReqData]): # 요청 데이터 형태 규정 -> 유효성 검사 자동 수행
    # 1. users를 순회(반복)하여 1명의 유저 정보 획득 => for
    results = list()
    for user in users:
        '''
            가상 공식
            사전식 = (소득//1000)*10
            credit_score = min(난수(300,600) + 사전식 , 990)
            grade = credit_score가 800 이상이면 A, 600 이상이면 B, 나머지 C
        '''
        # 2. 고객 1명 당 신용평가 수행 (AI x, 가상 공식 적용) => 차후 실제 모델로 교체
        사전식 = (user.income // 1000) * 10
        credit_score = min(random.randint(300,600) + 사전식 , 990)
        grade = "A" if credit_score >= 800 else "B" if credit_score >= 600 else "C"
        # 3. 평가 결과 담기 -> list
        results.append({
            "user_id": user.user_id,
            "credit_score":credit_score, # 0점 ~ 1000점
            "grade":grade
        })
        
    # 4. 결과 반환 
    return results  

