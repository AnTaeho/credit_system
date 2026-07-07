# Minor 해결 3건 (참고)

상태 요약: [docs/PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md)

### H2 의존성 강등
MySQL 앱에 섞여 있던 `spring-boot-h2console` 제거, H2는 테스트 인메모리 DB로만 쓰이므로
`testRuntimeOnly`로 강등.

커밋: `89d0ecb chore: demote H2 to test scope, align Kafka partition/concurrency settings`

### Kafka 파티션/동시성 정합성
`app.kafka.partitions`를 단일 진실원천으로 토픽 파티션 수(3)·`@KafkaListener` concurrency·
heartbeat 풀 크기를 모두 연동. 테스트 프로필은 1로 두어 1파티션 전제(DLT 블로킹 검증)를 보존.

커밋: `89d0ecb chore: demote H2 to test scope, align Kafka partition/concurrency settings`

### `attachJobId` 반환값 검증
`HoldService`에서 갱신 행 수가 1이 아니면 `IllegalStateException`으로 전체 롤백(0행 무통과 차단).

커밋: `82a2261 refactor: validate attachJobId result, extract BaseEntity, split HoldService responsibilities`
