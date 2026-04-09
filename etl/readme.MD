# 개요
  - 데이터 수집 level1에 대한 예제
  - openai 활용하여 ETL 진행
  - mysql은 docker 기반으로 구성 -> PC 가동시 -> docker 가동, mysql 가동 필수

# 설치
  - 파이썬 
    - 가상환경 구축
        - 가상환경 활성화
        - 필요 패키지 설치 
        - 구동
    - (*)기본 파이썬 환경 구축
        - 필요 패키지 한번에 모두 설치
        ```
            cd 현재 프로젝트 디렉토리
            pip install -r requirements.txt
        ```
        - 구동
  - mysql 설치
    - docker 설치를 가정하고 진행
    - 설치 (가상환경에서 데이터베이스를 설치했다)
    ```
      # --name : 컨테이너 이름
      # -e : 환경변수 설정, MYSQL_ROOT_PASSWORD root 사용자 비번 설정
      # -d : 백그라운드 가동
      # -p : 인바운드:아웃바운드
      # 이미지명 -> hub.docker.com에서 오피셜로 관리되는 네이밍
      docker run --name mysql -e MYSQL_ROOT_PASSWORD=1234 -d -p 3306:3306 mysql:latest
    ```


# 테스트 구동

# 구조
/
L .env              # 환경변수 파일
L .gitignore         # git 업로드시 생략될 파일 기술
L .news_lv2.py      # 파이썬 파일, 엔트리 포인트
L readme.MD         # 프로젝트 설명 문구
L requirments.txt   # 프로그램 구동시 필요한 패키지 목록

# 리뷰
  - import urllib or [v]import urllib.request
    urllib.request.Request() 사용시 최대한 경로까지 import 처리
  - pymysql 사용시 cryptography 패키지까지 세트로 설치 필요
    - 차후 가상환경 구성시 패키지 설치할 때 면밀한 검토 필요함 -> 파이썬만 있는 환경에서 구성하면 필요한 모든 패키지를 파악할 수 있음(코랩, 아나콘다 환경에서는 누락할 수 있음)
  - dotenv 사용

# 자동화  
  - 일단 생략(추후 적용)