'''
- 원격 PC에서 AWS S3에 데이터를 업로드하는 간단한 DAG
    - 액세스 키가 잘 작동하는지 체크
    - 데이터량에 따른 수행시간 체크 -> 데이터를 S3에 적제하는 방식에 대한 고민 (직접 or 서비스 이용)\
- 설치(호스트 PC, 로컬 PC 상)
    - pip install apache-airflow-providers-amazon
'''
# 1. 모듈 가져오기