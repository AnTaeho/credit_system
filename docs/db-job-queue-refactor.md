# DB 작업 큐 버전

이 브랜치는 Kafka, outbox relay, DLT를 제거하고 `jobs` 테이블을 영속 작업 큐로 사용한다.

## 처리 흐름

1. 생성 요청 트랜잭션에서 크레딧 `HOLD`, job(`HOLDING`), ledger를 함께 저장한다.
2. `GenerationWorker`가 일정 주기마다 `HOLDING` job을 최대 `app.worker.batch-size`건(기본 20건) 조회하고 조건부 UPDATE로 `PROCESSING` 상태를 선점한다.
3. 선점한 작업은 scheduler와 분리된 bounded executor에서 최대 `app.worker.concurrency`건(기본 3건) 실행한다. executor 대기열은 사용하지 않으며, 남은 작업은 DB의 `HOLDING`에 둔다.
4. 실행 워커만 Redis heartbeat를 등록한 뒤 외부 생성 작업을 호출한다.
5. 성공하면 `COMPLETED`, 외부 생성 실패는 `FAILED`로 기록한다. 결과 DB 반영 실패는 짧게 재시도하고, 그래도 실패하면 `PROCESSING`에 남겨 timeout 회수 경로에 맡긴다. 이때 중복 생성이 차단되는 것은 아니고 회수 시점까지 지연될 뿐이며, 이미 만들어진 결과 URL은 유실된다.
6. `DeadJobSchedulerTask`는 heartbeat가 없어진 오래된 `PROCESSING` job을 `FAILED`로 회수한다.
7. 실패 작업은 attemptNo를 증가시켜 다시 `HOLDING`으로 두거나, 시도 횟수를 모두 사용하면 환불한다.

## 보장 경계

크레딧 차감, job 생성, ledger 기록은 하나의 RDB 트랜잭션이다. 작업 큐도 같은 DB에 있으므로 DB와 Kafka 사이의 이중 쓰기·outbox 재발행 문제가 없다. 여러 워커가 있어도 `HOLDING → PROCESSING` 조건부 UPDATE와 attemptNo fencing으로 하나의 시도만 결과를 반영한다. `jobs(status, id)` 인덱스는 배치 폴링의 상태 필터와 ID 정렬을 지원한다.

`HOLDING`은 메시지 발행 실패가 아닌 정상적인 DB 대기열 상태이므로, 경과 시간만으로 실패·환불 처리하지 않는다. worker가 중지된 상태에서 남은 HOLDING job은 worker가 다시 기동되면 처리된다.

Redis는 작업 전달 수단이 아니라 살아있는 처리 작업을 확인하는 heartbeat 저장소로만 남는다. Redis 장애 시 기존 보수적 회수 정책을 유지한다.
