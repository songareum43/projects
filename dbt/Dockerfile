# Dockerfile
# 파이썬 이미지
FROM python:3.11-slim

# 리눅스 패키지등 최신 업데이트
RUN apt-get update && apt-get install -y \
    gcc default-libmysqlclient-dev pkg-config \
    && apt-get clean
# dbt-mysql 설치
RUN pip install --no-cache-dir dbt-core dbt-mysql

# 작업 디렉토리를 아래 폴더로 지정
WORKDIR /usr/app/dbt

# 컨테이너가 가동되면 dbt 명령어를 수행하시오
# ENTRYPOINT ["dbt"]