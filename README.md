# Credit System 프로젝트 정리본

Organization이 공유하는 크레딧을 선결제/차감하고, 비동기 이미지 생성(stub) 실패 시 정확히 환불하는 것을
목표로 한 포트폴리오 프로젝트다. 핵심 주장은 "크레딧은 항상 정확하게 차감·환불된다"이며, 이를
check-then-act 대신 **조건부 UPDATE/INSERT 하나로 확인+실행을 원자화**하는 설계 원칙과 attemptNo
fencing으로 보장하고, Testcontainers/EmbeddedKafka 기반 동시성·E2E 테스트(총 58건)로 증명한다.
이미지 생성 자체는 관심사가 아니므로 지연+확률적 실패를 가진 `GenerationStubClient`로 대체돼 있다.

## 1. 프로젝트 개요

**요구사항**
- 여러 Organization, 각 Organization에 여러 사용자, 사용자는 소속 Organization의 크레딧 잔액을 공유
- 크레딧은 선결제, 이미지 생성 시 차감, 잔액 부족 시 생성 시작 불가
- 이미지 생성은 비동기, 실패 시 사용된 크레딧 환불
- 동일 요청이 네트워크 이슈로 중복 전송될 수 있음(멱등성 필요)

**핵심 불변식**
> 크레딧은 항상 정확하게 차감·환불되어야 한다. 이중 차감도, 환불 유실도 허용하지 않는다.

**stub 범위**: 실제 이미지 생성 모델 연동 대신 `GenerationStubClient`가 지연 + 확률적 실패를 흉내낸다.
이미지 생성 로직 자체의 품질은 이 프로젝트의 검증 대상이 아니며, 그 결과를 크레딧 시스템이 정확히
반영하는지만 검증한다.

데모 계정 (최초 기동 시 `DataSeeder`가 자동 생성):

| username | password | organization | 초기 잔액 |
|---|---|---|---|
| alice | password123 | Acme Corp | 10,000 |
| bob | password123 | Globex Inc | 5,000 |

`/login`으로 로그인하면 `/dashboard`에서 잔액 확인, 충전, 이미지 생성 요청, job 상태(3초 폴링), ledger 내역을 볼 수 있습니다.

## 2. 기술 스택

Spring Boot 4.1 (Java 17) / Spring Data JPA / Spring Kafka / Spring Data Redis / Thymeleaf / MySQL /
H2(테스트 전용, `testRuntimeOnly`) / Testcontainers(MySQL, Redis) / EmbeddedKafka

## 3. 아키텍처와 처리 흐름

```
[클라이언트] --POST /api/jobs(idemKey)--> [HoldService]
                                              │  1. idempotency_keys INSERT (unique 위반 = 중복)
                                              │  2. organization.balance 조건부 차감 UPDATE
                                              │  3. job(HOLDING) / ledger(HOLD) / outbox INSERT
                                              ▼
                                        [OutboxRelay] (폴링)
                                              │  send().get(10s)로 브로커 ack 확인 후 markSent
                                              ▼
                                       Kafka: generation-jobs 토픽
                                              ▼
                                     [GenerationWorker] (컨슈머)
                                              │  Redis heartbeat 등록(ZADD) + job PROCESSING 전이
                                              │  GenerationStubClient.generate() 호출
                                              ├─ 성공 → ConfirmService (job COMPLETED, ledger CONFIRM)
                                              └─ 실패 → FailureService (job FAILED)
                                              ▼
                                    [DeadJobSchedulerTask] (스케줄러, 주기 폴링)
                                              │  reapStaleHolding: 정체된 HOLDING → FAILED 회수
                                              │  reapStaleProcessing: heartbeat 없는 PROCESSING → FAILED 회수
                                              ▼
                                     FAILED job 재검토
                                       ├─ attempt_no < 3 → RetryService: attempt_no+1, outbox 재투입
                                       └─ attempt_no 소진 → RefundService.finalRefund
                                                              (job REFUNDED, balance 환불, ledger REFUND)
```

poison message(반복 실패)는 Kafka `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`가
`generation-jobs.DLT`로 격리해 정상 메시지 처리를 막지 않는다.

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

5개 테이블로 구성된다.

- **organization**: `id`, `balance`, `version`, `updated_at` — 잔액을 이 컬럼으로 직접 관리
- **job**: `id`, `org_id`, `status`(HOLDING/PROCESSING/COMPLETED/FAILED/REFUNDED), `attempt_no`(fencing
  토큰), `hold_amount`, `updated_at`(heartbeat 용도로도 사용)
- **idempotency_keys**: `id`, `org_id`, `idem_key`(org_id와 함께 unique), `job_id` — 자체 status 없이
  job.status를 참조
- **ledger**: `id`, `org_id`, `job_id`, `type`(HOLD/CONFIRM/REFUND/CHARGE), `amount`, `created_at` —
  insert-only, 수정 없음
- **outbox**: `id`, `job_id`, `payload`, `sent`, `created_at`

관계: `organization 1—N job`, `job 1—0/1 idempotency_keys`, `job 1—N ledger`, `job 1—0/1 outbox`

## 6. 신뢰성 장치

- **Outbox + 브로커 ack 확인**: `OutboxRelay`가 폴링해 Kafka 발행 후 `send().get(10s)`으로 ack를
  확인한 뒤에만 `markSent` — DB와 메시지 큐 간 불일치 방지
- **DLT 격리**: `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`(`KafkaConsumerConfig`)가
  poison message를 `generation-jobs.DLT`로 격리해 파티션이 막히지 않게 한다
  (검증: `GenerationWorkerDltTest`)
- **`reapStaleHolding`**: 오래 갱신되지 않은 HOLDING job을 FAILED로 회수 — outbox 발행 실패/유실로
  인한 영구 정체 방지
- **`reapStaleProcessing`**: `app.processing.timeout-seconds`(60초) 초과했는데 살아있는 heartbeat가
  없는 PROCESSING job을 FAILED로 회수 — Redis 장애, DLT 격리 후 정체, heartbeat 등록 전 워커 크래시를
  모두 커버
- **Heartbeat**: Redis sorted set에 워커가 처리 시작 시점(job consume 시점)에 등록, 공유 스케줄러가
  주기 갱신 — job 수와 무관하게 O(1) 조회로 마감 지난 job만 스캔
- **Kafka 파티션 단일 진실원천**: `app.kafka.partitions`(운영 3, 테스트 1)가 토픽 파티션 수,
  `@KafkaListener` concurrency, heartbeat 풀 크기를 모두 연동해 정합성 유지

## 7. 프로젝트 구조

`src/main/java/com/example/credit_system` 기준 주요 패키지와 클래스:

```
com.example.credit_system
├── CreditSystemApplication.java
├── auth/                            # 로그인/세션 인증
│   ├── controller/LoginController.java
│   ├── domain/User.java
│   └── repository/UserRepository.java
├── dashboard/
│   └── DashboardController.java     # 잔액/충전/생성요청/폴링/ledger 뷰
├── global/
│   ├── auth/                        # LoginInterceptor, SessionConst
│   ├── config/
│   │   ├── AppProperties.java       # app.kafka.partitions, app.processing.timeout-seconds 등
│   │   ├── DataSeeder.java          # alice/bob 데모 계정 시딩
│   │   ├── KafkaConsumerConfig.java # DefaultErrorHandler + DeadLetterPublishingRecoverer
│   │   ├── KafkaTopicConfig.java
│   │   ├── PasswordEncoderConfig.java
│   │   └── WebConfig.java
│   ├── domain/BaseEntity.java       # 공통 엔티티 베이스(감사 필드 등)
│   ├── exception/                   # GlobalExceptionHandler 등
│   └── scheduler/
│       ├── DeadJobSchedulerTask.java  # reapStaleHolding / reapStaleProcessing / 재시도·최종환불 투입
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
│   │   ├── RetryService.java                   # attempt_no 증가 후 재투입
│   │   └── RefundService.java                  # 최종 환불(finalRefund)
│   ├── stub/GenerationStubClient.java, StubGenerationException.java
│   └── worker/GenerationWorker.java            # Kafka 컨슈머
├── ledger/
│   ├── controller/LedgerApiController.java
│   ├── domain/LedgerEntry.java, LedgerType.java
│   ├── dto/LedgerResponse.java
│   └── repository/LedgerRepository.java
├── organization/
│   ├── controller/OrganizationApiController.java
│   ├── domain/Organization.java
│   ├── dto/BalanceResponse.java, ChargeRequest.java
│   ├── repository/OrganizationRepository.java  # deductBalance / addBalance 조건부 UPDATE
│   └── service/ChargeService.java
└── outbox/
    ├── domain/GenerationJobMessage.java, OutboxEntry.java
    ├── repository/OutboxRepository.java
    └── service/OutboxRelay.java, OutboxWriter.java
```


