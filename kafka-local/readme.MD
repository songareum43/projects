# 주제
- kafka

# repo
- https://github.com/edu-kenshi/kafka-local.git

# 설치
```
    docker compose up -d
```

# 패키지 설치
```
pip install kafka-python
```

# CLI 모드로 테스트
```
# 접속
# 호스트  PC
PS ~\kafka-local> docker container exec -it kafka bash

# 컨테이너
root@4527e52ba5f6:/# pwd
/
root@4527e52ba5f6:/# cd /opt/kafka/bin

# 토픽 생성 (파티션 11개, 원본 1개만 유지)
> kafka-topics.sh --create --topic spacex --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1

# 토픽 전송(프로듀서 - 콘솔에서 데이터를 보낼 수 있는 입력 도구 실행)
> kafka-console-producer.sh --topic spacex --bootstrap-server 127.0.0.1:9092

# 컨슈머(데이터를 읽는 청취 도구 실행)
> kafka-console-consumer.sh --topic spacex --bootstrap-server 127.0.0.1:9092
```