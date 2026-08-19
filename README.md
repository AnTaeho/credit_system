# Credit System 프로젝트 정리본

Organization이 공유하는 크레딧을 선결제/차감하고, 비동기 이미지 생성(stub) 실패 시 정확히 환불하는 것을
목표로 한 포트폴리오 프로젝트다. 핵심 주장은 "크레딧은 항상 정확하게 차감·환불된다"이며, 이를
check-then-act 대신 **조건부 UPDATE/INSERT 하나로 확인+실행을 원자화**하는 설계 원칙과 attemptNo
fencing으로 보장하고, Testcontainers 기반 동시성·E2E 테스트를 포함한 총 100건의 테스트로 증명한다.
이미지 생성 자체는 관심사가 아니므로 지연+확률적 실패를 가진 `GenerationStubClient`로 대체돼 있다.

## 1. 프로젝트 개요

**요구사항**
- 여러 Organization이 존재하며, 각 Organization은 크레딧 잔액을 조직 단위로 공유
- 크레딧은 선결제, 이미지 생성 시 차감, 잔액 부족 시 생성 시작 불가
- 이미지 생성은 비동기, 실패 시 사용된 크레딧 환불
- 동일 요청이 네트워크 이슈로 중복 전송될 수 있음(멱등성 필요)

**핵심 불변식**
> 크레딧은 항상 정확하게 차감·환불되어야 한다. 이중 차감도, 환불 유실도 허용하지 않는다.

**stub 범위**: 실제 이미지 생성 모델 연동 대신 `GenerationStubClient`가 지연 + 확률적 실패를 흉내낸다.
이미지 생성 로직 자체의 품질은 이 프로젝트의 검증 대상이 아니며, 그 결과를 크레딧 시스템이 정확히
반영하는지만 검증한다.

API는 HTTP 헤더 `X-Organization-Id`로 조직을 식별한다. 웹 UI와 인증은 이 프로젝트의 범위 밖이다.

## 2. 기술 스택

Spring Boot 4.1 (Java 17) / Spring Data JPA / Spring Data Redis / MySQL /
H2(테스트 전용, `testRuntimeOnly`) / Testcontainers(MySQL, Redis)

## 3. 아키텍처와 처리 흐름

```
[클라이언트] --POST /api/jobs(idemKey)--> [HoldService]
                                              │  1. idempotency_keys INSERT (unique 위반 = 중복)
                                              │  2. organization.balance 조건부 차감 UPDATE
                                              │  3. job(HOLDING) / ledger(HOLD) INSERT
                                              ▼
                                    jobs 테이블 = 영속 작업 큐 (status=HOLDING)
                                              ▼
                                     [GenerationWorker] (@Scheduled 폴링, 기본 500ms)
                                              │  HOLDING job을 app.worker.batch-size(20)건까지 조회
                                              │  startProcessingIfAttemptMatches 조건부 UPDATE로 선점
                                              │  전용 executor(concurrency 3, 내부 큐 없음)로 위임
                                              ▼
                                    [GenerationJobProcessor] (워커 스레드)
                                              │  Redis heartbeat 등록(ZADD) + 주기 갱신
                                              │  GenerationStubClient.generate() 호출
                                              ├─ 성공 → ConfirmService (job COMPLETED, ledger CONFIRM)
                                              ├─ 생성 실패 → FailureService (job FAILED)
                                              └─ 결과 반영 실패 → 3회 재시도 후 PROCESSING 유지(회수 경로에 위임)
                                              ▼
                                    [DeadJobSchedulerTask] (스케줄러, 기본 5초 주기)
                                              │  heartbeat 만료 PROCESSING → FAILED 회수
                                              │  reapStaleProcessing: heartbeat 없는 PROCESSING → FAILED 회수
                                              ▼
                                     FAILED job 재검토
                                       ├─ attempt_no + 1 < 3 → RetryService: attempt_no+1 후 HOLDING으로 큐 재투입
                                       └─ attempt_no 소진 → RefundService.finalRefund
                                                              (job REFUNDED, balance 환불, ledger REFUND)
```

작업 큐가 DB 안에 있으므로 크레딧 차감·job 등록·ledger 기록이 하나의 트랜잭션으로 커밋되고, DB와
브로커 사이의 이중 쓰기 문제가 없다. 반복 실패하는 작업은 `app.generation.max-attempts`(3) 상한에
걸려 환불로 종결되므로 큐에 영구히 남아 처리량을 잠식하지 않는다.

## 4. 핵심 설계 원칙

> "확인 후 처리(check-then-act)" 구조를 쓰지 않는다. 모든 상태 변경은 **조건부 UPDATE/INSERT 하나**로
> 확인과 실행을 원자화한다. `credit_system_design.md`의 초기 설계는 낙관적 락(`version` 비교)을
> 전제했으나, 실제 구현은 이보다 더 단순한 **`balance >= :amount` 조건부 UPDATE**로 대체해 재시도
> 루프 자체를 제거했다(Blocker ① 해결, 아래 5절 참고).

| 문제 상황 | 조건부 연산(현재 코드 기준) |
|---|---|
| 동시 잔액 차감 | `UPDATE organization SET balance = balance - :amount WHERE id = :id AND balance >= :amount` (`OrganizationRepository.deductBalance`) — 0행이면 잔액 부족으로 즉시 실패, 재시도 루프 없음 |
| 잔액 증액(환불/충전) | `UPDATE organization SET balance = balance + :amount WHERE id = :id` (`addBalance`) — 조건 없이 무조건 성공 |
| 이중 결과 쓰기(재시도된 워커 vs 원래 워커) | `UPDATE job SET status=? WHERE id=? AND attempt_no=?` — attemptNo fencing |
| 중복 요청 | `idempotency_keys`의 `(org_id, idem_key)` DB unique 제약 위반을 중복 판정으로 사용 (SELECT 후 INSERT 아님) |
| 최종 refund 시점 늦은 워커의 성공 응답 경합 | `UPDATE job SET status='REFUNDED' WHERE id=? AND status='FAILED' AND attempt_no=?` — 0행이면 늦은 워커가 이미 COMPLETED로 바꿔놓은 것이므로 환불 취소 |
| 스케줄러 다중 인스턴스의 재시도 투입 경합 | `UPDATE job SET attempt_no=attempt_no+1 WHERE id=? AND status='FAILED' AND attempt_no=?` |

## 5. 데이터 모델

4개 테이블로 구성된다.

- **organization**: `id`, `balance`, `updated_at` — 잔액을 이 컬럼으로 직접 관리
- **job**: `id`, `org_id`, `status`(HOLDING/PROCESSING/COMPLETED/FAILED/REFUNDED), `attempt_no`(fencing
  토큰), `hold_amount`, `updated_at`(heartbeat 용도로도 사용) — 작업 큐를 겸하므로 워커의 배치 폴링
  (status 필터 + id 정렬)을 위해 `idx_jobs_status_id(status, id)` 인덱스를 둔다
- **idempotency_keys**: `id`, `org_id`, `idem_key`(org_id와 함께 unique), `job_id` — 자체 status 없이
  job.status를 참조
- **ledger**: `id`, `org_id`, `job_id`, `type`(HOLD/CONFIRM/REFUND/CHARGE), `amount`, `created_at` —
  insert-only, 수정 없음

관계: `organization 1—N job`, `job 1—0/1 idempotency_keys`, `job 1—N ledger`

## 6. 신뢰성 장치

- **DB 작업 큐 + 조건부 선점**: 크레딧 차감·job(HOLDING)·ledger가 한 트랜잭션으로 커밋되므로 큐 투입이
  유실될 수 없다. 워커는 `startProcessingIfAttemptMatches`(`status='HOLDING' AND attempt_no=?` 조건부
  UPDATE)로 선점하고, 0행이면 다른 워커가 이미 가져간 것으로 보고 건너뛴다 — 같은 목록을 여러 인스턴스가
  읽어도 실행은 하나뿐
- **HOLDING은 회수 대상이 아님**: HOLDING은 발행 실패 상태가 아니라 정상적인 큐 대기 상태이므로 경과
  시간만으로 실패 처리하지 않는다. 워커가 꺼져 있는 동안 쌓인 job은 재기동 후 다음 폴링에서 그대로 처리된다
- **`reapStaleProcessing`**: `app.processing.timeout-seconds`(60초) 초과했는데 살아있는 heartbeat가
  없는 PROCESSING job을 FAILED로 회수 — Redis 장애, 선점 직후 executor 위임 실패와 롤백 실패, heartbeat
  등록 전 워커 크래시, 결과 반영(confirm) 재시도 소진을 모두 커버
- **Heartbeat**: Redis sorted set에 워커가 실제 실행을 시작하는 시점(`GenerationJobProcessor.process`)에
  등록하고 `app.heartbeat.refresh-interval-seconds`(5초)마다 갱신 — job 수와 무관하게 O(1) 조회로 마감
  지난 job만 스캔. Redis 장애 중에는 회수를 억제하되, 억제가
  `app.heartbeat.suppression-alert-seconds`(60초)를 넘기면 ERROR로 경보한다
- **워커 동시 실행 상한**: `app.worker.concurrency`(3)가 전용 executor(`generationWorkerExecutor`)의
  스레드 수와 `Semaphore` permit 수를 함께 결정하고, executor 내부 큐 용량은 0이다 — 대기열 역할은 DB의
  HOLDING 상태가 하므로 선점만 해두고 실행되지 않는 job이 생기지 않는다. `app.worker.batch-size`(20)는
  한 폴링 주기의 조회 상한이고, `spring.task.scheduling.pool.size`(2)는 워커 폴링과
  `DeadJobSchedulerTask`가 같은 스케줄러 스레드를 두고 경합하지 않게 한다. 두 값은 `WorkerProperties`가
  기동 시점에 검증한다(1 미만이면 시작 실패)

## 7. 프로젝트 구조

`src/main/java/com/example/credit_system` 기준 주요 패키지와 클래스:

```
com.example.credit_system
├── CreditSystemApplication.java
├── global/
│   ├── config/
│   │   └── AppProperties.java       # app.generation / stub / heartbeat / processing 바인딩·검증
│   ├── domain/BaseEntity.java       # 공통 엔티티 베이스(감사 필드 등)
│   ├── exception/                   # GlobalExceptionHandler 등
│   └── scheduler/
│       ├── DeadJobSchedulerTask.java  # heartbeat 만료·reapStaleProcessing 회수 / 재시도·최종환불 투입
│       └── HeartbeatRegistry.java    # Redis sorted-set heartbeat
├── job/
│   ├── controller/JobApiController.java
│   ├── domain/Job.java, JobStatus.java, IdempotencyKey.java
│   ├── dto/JobCreateRequest.java, JobCreateResponse.java, JobResponse.java
│   ├── repository/JobRepository.java, IdempotencyKeyRepository.java
│   ├── service/
│   │   ├── HoldService.java, HoldResult.java   # 요청 접수(hold) 트랜잭션
│   │   ├── ConfirmService.java                 # 성공 확정
│   │   ├── FailureService.java                 # 실패 전이
│   │   ├── RetryService.java                   # attempt_no 증가 후 HOLDING 재투입
│   │   └── RefundService.java                  # 최종 환불(finalRefund)
│   ├── stub/GenerationStubClient.java, StubGenerationException.java
│   └── worker/
│       ├── GenerationWorker.java               # DB 큐 폴링 + 조건부 UPDATE 선점
│       ├── GenerationJobProcessor.java         # 선점된 작업의 외부 호출·결과 반영
│       ├── WorkerExecutorConfig.java           # generationWorkerExecutor(큐 없는 bounded pool)
│       └── WorkerProperties.java               # app.worker.* 바인딩·검증
├── ledger/
│   ├── controller/LedgerApiController.java
│   ├── domain/LedgerEntry.java, LedgerType.java
│   ├── dto/LedgerResponse.java
│   └── repository/LedgerRepository.java
└── organization/
    ├── controller/OrganizationApiController.java
    ├── domain/Organization.java
    ├── dto/BalanceResponse.java, ChargeRequest.java
    ├── repository/OrganizationRepository.java  # deductBalance / addBalance 조건부 UPDATE
    └── service/ChargeService.java
```


