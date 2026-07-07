# Kafka 컨슈머 에러 처리 부재 (Major) — `d3cbe73`

상태 요약: [docs/PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md)

### P — Problem
컨슈머에 에러 핸들러가 없어, 파싱 실패(poison message)나 반복 실패 예외가 발생하면
해당 레코드에서 무한 재시도/정지해 **파티션 전체가 막힐 수 있었다.** 뒤따르는 정상
메시지도 처리되지 못한다.

### A — Analysis
Kafka는 파티션 내 순서를 보장하므로, 앞 레코드가 진행하지 못하면 **뒤 레코드 전부가
블로킹**된다. 재시도로 해결 안 되는 예외(역직렬화 실패 등)는 아무리 재시도해도 영영
못 넘어가므로, 유한 횟수 시도 후 **격리(DLT)** 해서 파티션을 흘려보내야 한다.

### Action
`KafkaConsumerConfig`에 `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` 구성.

```java
DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
        (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
```

- 최초 1회 + 1초 간격 2회 재시도 = 총 3회 시도 후 `generation-jobs.DLT`로 격리.
- 기본 목적지 리졸버는 `<topic>-dlt`(하이픈) 규칙이라, `<topic>.DLT` 토픽으로 보내려면
  리졸버를 명시(위 람다).

### R — Result
- poison/반복 실패 메시지가 파티션을 막지 않고 DLT로 격리 → 후속 정상 메시지 정상 처리.
- DLT 격리된 메시지의 job이 PROCESSING에 남는 문제는 5번 reaper가 회수.
- 검증 `GenerationWorkerDltTest`(1파티션 토픽): poison 후속의 정상 메시지가 처리됨
  (비블로킹) + poison이 DLT에 실제 수신됨을 단언.
