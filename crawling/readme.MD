# 개요
    - chromedriver를 작동
        - [v] 직접 다운로드 후 세팅
        - 알아서 자동으로 버전에 맞춰 세팅
    - 프로그래밍된 내용으로 chromedriver에서 사이트에 액션 전개됨
    - 데이터 수집

# 패키지    
    - pip install selenium
    - 4.41.0 버전 사용(현재)

# Flow
  - Extract
    - 주요소 가격 사이트에서 => 시도/시군구 별로 엑셀 다운
    - 엑셀 파일들을 전부 모아서 DataFrame으로 load 처리
  - Transform
    - 엑셀 파일들을 전부 모아서 DataFrame으로 load 처리
    - 전처리 x, 정제 x...
  - Load
    - RDB Database or s3에 저장
  - 예시(데이터파이프라인) 
    - 매일 밤 12시 1분에 진행 (일 1회) -> 주기성 -> airflow 처리 가능 
    - s3 업로드 -> Athena 쿼리 -> 분석 결과 등 획득 가능 -> 대시 구성 가능 -> 아침 레포트 전송 