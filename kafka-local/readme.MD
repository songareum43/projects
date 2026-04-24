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

# local-datapipline 목표 시스템
```
[ LAYER ]          [ COMPONENT ]              [ PROTOCOL / ACTION ]             [ DATA FORMAT ] 
====================================================================================================
SOURCE       :     Python (log_gen.py)  ----( Write to File )---->  [ /sensor_logs/*.log ]
               |                                                                   |
               |                                                            ( Shared Volume )
               v                                                                   |
INGESTION    :     Fluent Bit (Agent)   <---( Tailing File  )----------------------+
               |          |
               |          | [ CHANNEL A: DIRECT BYPASS ]
               |          +--------------------------------------------( Produce )--+
               |          |                                                         |
               |          | [ CHANNEL B: PROXY ROUTE  ]                             |
               |          +----( Forward )----> [ Logstash ] ----( Produce )--+     |
               |                                     |                        |     |
               v                                     v                        v     v
TRANSFORM    : (A: Bypass)  +--[ Grok    : Pattern Matching & Extract Fields   ]--+ |
               |            +--[ Mutate  : Data Type Conversion (Str->Float)   ]--+ |
               |            +--[ Tagging : Add Metadata for Conditional Alerts ]--+ |
               |                                                              |     |
MESSAGING    :     Kafka / MSK (Broker) <-------------------------------------+-----+
               |          |             ( Merge Structured & Processed Data )
               |          |
               |          +----( Topic A: factory-json-topic ) <--- [Channel A Result]
               |          +----( Topic B: factory-text-topic ) <--- [Channel B Result]
               v                                                               |
                                                                          kafka connect / ui
                                                                               |
                                                                               v
VISUALIZE    :     OpenSearch (DB)      <---( Indexing & Search )---->  DASHBOARD (Kibana)
====================================================================================================
```
```
[ 계층 ]            [ 구성 요소 ]              [ 프로토콜 / 동작 ]              [ 데이터 형식 ]
====================================================================================================
데이터 소스  :     파이썬 (log_gen.py)  ----( 파일 쓰기 동작 )---->  [ /sensor_logs/*.log ]
               |                                                                   |
               |                                                            ( 공유 볼륨 연결 )
               v                                                                   |
데이터 수집  :     Fluent Bit (에이전트) <---( 로그 실시간 추적 )----------------------+
               |          |
               |          | [ 채널 A: 최단거리 직결 ]
               |          +--------------------------------------------( 메시지 발행 )--+
               |          |                                                           |
               |          | [ 채널 B: 가공 처리 중계 ]                                   |
               |          +----( 데이터 전달 )----> [ Logstash ] ----( 메시지 발행)--+    |
               |                                       |                          |    |
               v                                       v                          v    v
데이터 가공  : (A: 통과)    +--[ Grok    : 패턴 매칭을 통한 의미 있는 필드 추출 ]---+         |
               |            +--[ Mutate  : 데이터 타입 변환 (문자열 -> 실수형)  ]---+      |
               |            +--[ Tagging : 조건별 알람 처리를 위한 메타데이터  ]---+        |
               |                                                                  |    |
데이터 적재  :     Kafka / MSK (허브)   <------------------------------------------+--+
               |          |             ( 직결 데이터와 가공 데이터 통합 수신 )
               |          |
               |          +----( 토픽 A: factory-json-topic ) <--- [채널 A 결과물]
               |          +----( 토픽 B: factory-text-topic ) <--- [채널 B 결과물]
               v                                                                  | 
                                                                          kafka connect / ui
                                                                                  |
                                                                                  v
시각화/분석  :     OpenSearch (DB)      <---( 인덱싱 및 검색 )------>  대시보드 (Kibana)
====================================================================================================
```

# 프로젝트 재구성
- docker-compose.yaml 수정(KRaft 적용)
```
    docker compose down
    docker compose up -d
```

# Fluentd(40~60MB) <-> Fluent-Bit(1~5MB)
- 태그 기반 라우팅 도구(Tag Based Routing)
- Ruby(C 결합) 구조 <-> C
- fluent-bit/etc/fluent-bit.conf에서 기술 구성 완료
- 목표
    - 데이터 수집, 처리, 전달 => ETL 도구
    - 경량형 오픈 소스 로그 프로세서
    - 고가용성, 고성능 처리, 데이터 처리 안정성
- 파이프라인
    - input  : 파일, 시스템 매트릭, http 스트림.. 다양한 소스 제공
    - service, parser, filter, buffer, rountiong
    - output : kafka/opensearch/elasticsearch/s3/.. 모두 가능
- 지원 플러그인
    - 약 1000개 <-> 약 100개

- conf 구성 => 규칙 기술
    ```
    - docker compose restart
    ```

- 컨슈머 구동
```
    # json 관련 컨슈머
    PS .. > docker exec -it kafka-local bash
    $ > ./opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic factory-json-topic --from-beginning

    # text 관련 컨슈머
    PS .. > docker exec -it kafka-local bash
    $ > ./opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic factory-text-topic --from-beginning
```

- conf가 수정되면
    - `docker compose restart` 반드시 수행

- 전달 내용
    - http로 전달해서 데이터가 늘어남
    - forward 등 전달 방식은 다양
        - forward 시도, 24224 포트 사용 -> http와는 다른 모양으로 나옴

# 로그 형태
- 참고
    - 최초 데이터 => fluent-bit/logstash => 데이터 형태에 노이즈가 많음(프로토컬 때문에 추가된 내용이 많음) => 필터 역할 필요
    - 필터 보정하는 내용 추후 업데이트

# kafka 대시보드
- kafka ui
- 메세지의 흐름, connect를 통해 전달이 잘 되는지 등 모니터링
 
 # 로컬 -> 클라우드 연결
- kafka connect
- docker-compose.yaml 업데이트
```
    docker compose down
    docker compose up -d
```
- CLI 등록
```
curl -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d '{
            "name": "kafka-to-firehose-sink",
            "config": {
            "connector.class": "io.confluent.connect.aws.firehose.KinesisFirehoseSinkConnector",
            "tasks.max": "1",
            "topics": "factory-json-topic,factory-text-topic",
            "region": "ap-northeast-2",
            "aws.kinesis.firehose.delivery.stream": "de-ai-09-ap2-kafka",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true"
            }
        }'
``` 

```

```

- Kafka ui 접속
    - create connector
    - config
        - firehose 관련 내용
    ```
    name: kafka-to-firehose-sink (커넥터 식별 이름)
    connector.class: io.confluent.connect.aws.firehose.KinesisFirehoseSinkConnector
    tasks.max: 1 (데이터량이 많으면 숫자를 늘려 병렬 처리 가능)
    topics: factory-json-topic,factory-text-topic (구독할 토픽들)
    region: ap-northeast-2 (AWS 리전)
    aws.kinesis.firehose.delivery.stream: 자신의_파이어호스_스트림_이름
    value.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable: false
    ```
    ```
    {
        "name": "kafka-to-firehose-sink",
        "config": {
            "connector.class": "io.confluent.connect.aws.firehose.KinesisFirehoseSinkConnector",
            "tasks.max": "1",
            "topics": "factory-json-topic,factory-text-topic",
            "region": "ap-northeast-2",
            "aws.kinesis.firehose.delivery.stream": "de-ai-09-ap2-kafka",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true"
        }
    } 
    ```
