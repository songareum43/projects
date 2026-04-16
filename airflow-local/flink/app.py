'''
- pip install apache-flink==1.15.0
- 요구사항 => raw data에서 티커별로 10초 당 평균 가격 추출 => 다음 kinesis로 전달
- 입력 테이블, 출력 테이블, 조회 및 전송
- 표준 SQL + AWS + Flink 특징 추가된 형태 
'''

import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    pass

# 단독형 앱 => 엔트리 포인트 표기 필요
if __name__ == "__main__":
    main()