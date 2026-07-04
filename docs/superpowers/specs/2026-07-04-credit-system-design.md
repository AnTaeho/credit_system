# Credit System 구현 설계

> 이 문서는 `credit_system_design.md`(동시성 설계 원안)를 실제 Spring Boot 구현으로 옮기기 위한 구체화 설계다. 원안의 핵심 원칙(조건부 UPDATE로 check-then-act 제거)은 그대로 따르되, PK 타입/누락된 테이블/UI/테스트 전략 등 원안에 없던 부분을 이 문서에서 확정한다.

## 0. 프로젝트 성격

- 포트폴리오/연구용 프로젝트. 실사용 목적이 아니므로 이미지 생성 로직은 **stub**으로 대체한다 (핵심 어필 포인트가 아님).
- 핵심 어필 포인트는 **크레딧 차감/환불의 정확성**(동시성 안전성)이며, 이를 기술적으로 견고하게 구현하고 테스트로 증명하는 것이 목표다.
- 모든 테이블의 PK는 `Long` (MySQL `AUTO_INCREMENT` / JPA `GenerationType.IDENTITY`).

## 1. 요구사항 요약 (원안 그대로)

- 여러 Organization, 각 Organization에 여러 User, User는 소속 Organization의 크레딧 잔액을 공유
- 크레딧은 선결제, 이미지 생성 시 차감, 잔액 부족 시 생성 시작 불가
- 이미지 생성은 비동기, 수십 초 이상 소요
- 생성 실패 시 사용된 크레딧은 환불
- 동일 요청이 네트워크 이슈로 중복 전송될 수 있음
- **제약 조건: 크레딧은 항상 정확하게 차감·환불되어야 함**

## 2. 핵심 설계 원칙 (원안 그대로)

> "확인 후 처리(check-then-act)" 구조를 쓰지 않는다. 모든 상태 변경은 **조건부 UPDATE/INSERT 하나**로 확인과 실행을 원자화한다.

| 문제 상황 | 조건부 연산 |
|---|---|
| 동시 잔액 차감 | `UPDATE organization SET balance=?, version=version+1 WHERE id=? AND version=?` |
| 이중 결과 쓰기(재시도된 워커 vs 원래 워커) | `UPDATE job SET status=? WHERE id=? AND attempt_no=?` |
| 중복 요청 | `INSERT INTO idempotency_keys (...)` — unique 제약 위반을 중복 판정으로 사용 |
| 최종 refund 시점 늦은 워커의 성공 응답 경합 | `UPDATE job SET status='REFUNDED' WHERE id=? AND status='FAILED' AND attempt_no=?` |
| 스케줄러 다중 인스턴스의 재시도 투입 경합 | `UPDATE job SET attempt_no=attempt_no+1 WHERE id=? AND status='FAILED' AND attempt_no=?` |

이 낙관적 락은 **JPA `@Version`을 쓰지 않고**, `@Modifying @Query`로 직접 작성한 조건부 UPDATE의 반영 row 수(0/1)로 판정한다. JPA의 `OptimisticLockException` 기반 방식은 예외 처리 흐름이 원안의 "0행이면 재시도/실패 분기" 시맨틱과 다르므로 채택하지 않는다.

## 3. 아키텍처 개요

단일 Spring Boot 프로세스 안에서:

- **Web** (Thymeleaf + REST API) — 로그인, 대시보드, 생성 요청/충전 API
- **OutboxRelay** (`@Scheduled(fixedDelay=1000)`) — `outbox` 테이블에서 `sent=false` 행을 폴링해 Kafka로 발행 후 `sent=true` 처리
- **GenerationWorker** (`@KafkaListener`) — 큐 메시지를 소비해 stub 이미지 생성 호출 → confirm/fail 처리
- **DeadJobSchedulerTask** (`@Scheduled(fixedDelay=5000)`) — Redis heartbeat 만료 감지 + `FAILED` job 재시도/최종 환불

인프라는 `docker-compose.yml`로 MySQL + Kafka + Redis를 로컬에 띄운다. H2는 테스트에서만 사용한다(운영/개발은 MySQL).

### 기술 스택

Spring Boot 4.1 / Java 17, Spring Data JPA, Spring Kafka, Spring Data Redis, Thymeleaf, Spring Web MVC, Lombok(`@Getter`만 허용, `@Data`/`@Setter` 금지), MySQL(운영·개발) / H2(테스트).

## 4. 패키지 구조

`PROJECT_STRUCTURE_GUIDE.md` 원칙(도메인별 controller/domain/dto/repository/service 반복, 공통 관심사는 `global/`)을 그대로 적용한다.

```
src/main/java/com/example/credit_system/
├── auth/                 User, 세션 로그인
│   ├── controller/ domain/ dto/ repository/ service/
├── organization/         Organization, 잔액/버전 관리, 충전(Charge)
│   ├── controller/ domain/ dto/ repository/ service/
├── job/                  Job(hold/confirm/fail/retry/refund), IdempotencyKey
│   ├── controller/ domain/ dto/ repository/ service/
│   ├── worker/           GenerationWorker (@KafkaListener)
│   └── stub/             GenerationStubClient (이미지 생성 stub)
├── ledger/               LedgerEntry (insert-only 기록/조회)
│   ├── domain/ repository/ service/
├── outbox/               OutboxEntry + OutboxRelay(@Scheduled)
│   ├── domain/ repository/ service/
├── global/
│   ├── config/           Kafka, Redis, DataSource(profile별) 설정
│   ├── exception/        InsufficientBalanceException 등 커스텀 예외 + @RestControllerAdvice
│   └── scheduler/        DeadJobSchedulerTask, heartbeat RedisTemplate 래퍼
└── dashboard/            Thymeleaf 전용 컨트롤러 (대시보드/ledger 뷰)
```

`idempotency_keys`는 job과 강하게 결합되므로 `job/` 도메인 하위에 둔다.

## 5. 데이터 모델

모든 PK는 `Long` (`GenerationType.IDENTITY`).

**Organization**

| 필드 | 타입 | 비고 |
|---|---|---|
| id | Long (PK) | |
| name | String | |
| balance | Long | |
| version | Long | 낙관적 락 (수동 조건부 UPDATE, JPA `@Version` 미사용) |
| createdAt/updatedAt | Instant | |

**User** *(원안엔 없던 테이블, 이번에 추가)*

| 필드 | 타입 | 비고 |
|---|---|---|
| id | Long (PK) | |
| organizationId | Long (FK) | |
| username | String | unique |
| password | String | BCrypt |
| createdAt | Instant | |

**Job**

| 필드 | 타입 | 비고 |
|---|---|---|
| id | Long (PK) | |
| organizationId | Long (FK) | |
| status | Enum | HOLDING / PROCESSING / COMPLETED / FAILED / REFUNDED |
| attemptNo | int | fencing token |
| holdAmount | Long | |
| prompt | String | 생성 요청 원문 (원안 테이블엔 누락, worker/큐 payload 구성에 필요해 추가) |
| resultUrl | String, nullable | stub 결과 확인용 (대시보드 표시) |
| updatedAt | Instant | heartbeat |

**IdempotencyKey**

| 필드 | 타입 | 비고 |
|---|---|---|
| id | Long (PK) | |
| organizationId | Long (FK) | |
| idemKey | String | `(organizationId, idemKey)` unique |
| jobId | Long (FK, nullable) | job 생성 전엔 null |

**LedgerEntry**

| 필드 | 타입 | 비고 |
|---|---|---|
| id | Long (PK) | |
| organizationId | Long (FK) | |
| jobId | Long (FK, nullable) | CHARGE는 특정 job과 무관하므로 nullable |
| type | Enum | HOLD / CONFIRM / REFUND / CHARGE |
| amount | Long | 부호 있는 증감값 |
| createdAt | Instant | insert-only, update 없음 |

**OutboxEntry**

| 필드 | 타입 | 비고 |
|---|---|---|
| id | Long (PK) | |
| jobId | Long (FK) | |
| payload | String(JSON) | jobId/orgId/attemptNo/prompt |
| sent | boolean | |
| createdAt | Instant | |

관계: `Organization 1—N User`, `Organization 1—N Job`, `Job 1—0/1 IdempotencyKey`, `Job 1—N LedgerEntry`, `Job 1—N OutboxEntry`

**원안 대비 수정 사항**: 원안 문서는 `job 1—0/1 outbox`로 명시했지만, 재시도 의사코드(`finalRefund`/`retry`)에서 재시도마다 새 attempt로 outbox를 다시 insert하므로 실제로는 `job 1—N outbox`(시도당 1건)가 맞다. 이 문서에서는 이를 반영한다.

## 6. 핵심 플로우

### 6.1 Hold (생성 요청 접수) — `POST /api/jobs`

```
@Transactional
1. INSERT idempotency_keys(org_id, idem_key)
   → unique 위반(DataIntegrityViolationException) 캐치 시 기존 idem_key로 SELECT job_id → 같은 job 정보로 응답(중복 판정)
2. SELECT balance, version FROM organization WHERE id=?
   balance < holdAmount → InsufficientBalanceException (409)
3. @Modifying @Query
   UPDATE organization SET balance=balance-:amount, version=version+1
   WHERE id=:id AND version=:version
   → 반환값 0행이면 최대 3회까지 재조회 후 재시도, 3회 소진 시 ConflictException(409)
4. INSERT job(status=HOLDING, attempt_no=0, hold_amount, prompt)
5. UPDATE idempotency_keys SET job_id=?
6. INSERT ledger(type=HOLD, amount=-holdAmount)
7. INSERT outbox(job_id, payload={jobId, orgId, attemptNo=0, prompt}, sent=false)
COMMIT
```

### 6.2 Outbox → Kafka

`OutboxRelay`가 `@Scheduled(fixedDelay=1000)`으로 `sent=false` 행을 폴링 → `KafkaTemplate.send("generation-jobs", payload)` 성공 시 `sent=true` UPDATE. 재전송 시 컨슈머 측 idempotent 처리는 원안의 "향후 과제"이며, attempt_no 조건부 UPDATE가 사실상 그 역할을 겸하므로 별도 처리하지 않는다.

### 6.3 Worker (`@KafkaListener`)

```
consume(message: {jobId, orgId, attemptNo, prompt})
  redis.zadd("heartbeats", jobId, now+timeout)
  UPDATE job SET status=PROCESSING WHERE id=? AND attempt_no=?   (0행이면 즉시 리턴 — 무효 메시지)
  try:
    result = stubClient.generate(prompt)   // Thread.sleep(min~max) + 확률적 실패(failure-rate)
    confirmService.confirm(jobId, attemptNo, result)
  catch:
    failureService.markFailed(jobId, attemptNo)
  finally:
    redis.zrem("heartbeats", jobId)
```

`stubClient`는 `application.yml`의 `stub.failure-rate`, `stub.min-delay-sec`, `stub.max-delay-sec`로 동작을 제어하는 실제 구현체다(mock 아님). 테스트에서는 `application-test.yml`에서 이 값을 조절해 결정론적 결과를 만든다.

### 6.4 Confirm

```
@Transactional
UPDATE job SET status=COMPLETED, result_url=? WHERE id=? AND attempt_no=?
  → 0행이면 종료(이미 무효화된 시도)
INSERT ledger(type=CONFIRM, amount=0)
```

### 6.5 Fail → Retry / 최종 Refund (`DeadJobSchedulerTask`)

```
expired = redis.zrangeByScore("heartbeats", -inf, now)
targets = job WHERE (id IN expired) OR status=FAILED

for job in targets:
  if attempt_no < 3:
    UPDATE job SET attempt_no=attempt_no+1, status=PROCESSING
    WHERE id=? AND status=FAILED AND attempt_no=?
    → 성공 시 새 attempt_no로 outbox insert (재발행)
  else:
    @Transactional
    UPDATE job SET status=REFUNDED WHERE id=? AND status=FAILED AND attempt_no=?
      → 0행이면 늦은 워커가 이미 COMPLETED 처리, 종료
    UPDATE organization SET balance=balance+hold_amount, version=version+1 WHERE id=? AND version=?
      → 0행이면 재시도(최대 3회), 소진 시 ERROR 로그(운영 알림은 향후 과제)
    INSERT ledger(type=REFUND, amount=+hold_amount)
```

### 6.6 Charge (충전) — 원안엔 없던 흐름, 이번에 단순 구현

외부 PG 연동 없이 대시보드 "충전" 버튼 → `POST /api/organizations/{id}/charge`

```
@Transactional
UPDATE organization SET balance=balance+:amount, version=version+1 WHERE id=? AND version=?
  → 0행이면 재시도(최대 3회)
INSERT ledger(type=CHARGE, amount=+amount)
```

Hold/Refund와 동일한 조건부 UPDATE 패턴을 재사용하며 새 동시성 메커니즘을 도입하지 않는다.

### 6.7 Redis Heartbeat

- `heartbeats` sorted set, member=jobId(문자열), score=now+timeout(10s)
- 워커가 PROCESSING 전이 시 최초 등록, 이후 공유 스케줄러(`ScheduledExecutorService`, 5s 주기)가 갱신
- `HOLDING` 상태 job은 감시 대상 아님 (6.5의 target 조회에 자연히 빠짐 — heartbeat 미등록 + status≠FAILED)

## 7. UI / 인증

**인증**: Spring Security 없이 `HttpSession` 기반. `POST /login`에서 username/password(BCrypt) 검증 후 세션에 `userId`, `organizationId` 저장. `HandlerInterceptor`가 세션 존재 여부를 확인해 `/dashboard`, `/api/**` 접근 시 없으면 `/login`으로 리다이렉트(API는 401).

**화면/엔드포인트**

| 경로 | 설명 |
|---|---|
| `GET/POST /login` | 로그인 폼 + 처리 |
| `GET /dashboard` | 잔액, 충전 버튼, 생성 요청 폼, job 목록(초기 렌더) |
| `GET /api/jobs` | 현재 org의 job 목록 JSON (2~3초 폴링용) |
| `POST /api/jobs` | 생성 요청(Hold). idem key는 폼 로드 시 JS로 UUID 생성해 hidden input에 저장, 제출 버튼은 클릭 즉시 비활성화(중복 클릭 방지) |
| `POST /api/organizations/{id}/charge` | 충전 |
| `GET /api/ledger` | 현재 org의 ledger 내역 JSON (탭 클릭 시 조회) |

멱등성은 UI 클릭보다 **네트워크 재전송** 방지가 목적이므로, UI는 중복 클릭만 막는다. "동일 idem key로 실제 재전송 시 같은 job이 반환된다"는 동작은 README에 curl 예시로 남겨 리뷰어가 재현할 수 있게 한다.

Job 목록의 실시간 갱신은 SSE 대신 **클라이언트 폴링**(JS `setInterval`로 `/api/jobs` 재조회)을 쓴다. 핵심 어필 포인트는 크레딧 로직이지 이벤트 인프라가 아니므로, 폴링이 더 가볍고 목적에 부합한다.

## 8. 테스트 전략 (Mock 전면 금지, 3계층)

이 프로젝트는 **Mockito 등 mock 프레임워크를 어디에도 쓰지 않는다.** 가벼운 테스트는 최대한 가볍게(H2 인메모리), 무거운 테스트는 실제 인프라 그대로(Testcontainers) 무겁게 간다.

| Tier | 인프라 | 목적 | 예시 |
|---|---|---|---|
| **1. 가벼움** | H2 인메모리, 실제 Spring 빈(Service+Repository) | 분기 로직 검증 | `HoldServiceTest`: 잔액부족 시 예외 / `ConfirmServiceTest`: attempt_no 불일치 시 0행·상태 불변 |
| **2. 무거움** | Testcontainers MySQL | 실제 동시성 안전성 | `ConcurrentHoldTest`(N스레드 동시 hold, balance 음수 안됨) / `DuplicateIdemKeyTest`(동일 idem_key 동시요청, job 1개만 생성) / `RetryRefundTest`(3회 실패 후 최종 REFUNDED + balance 복구) |
| **3. 가장 무거움** | Testcontainers MySQL+Redis + EmbeddedKafka | 전체 파이프라인 실증 | hold → outbox relay 발행 → kafka 소비 → worker 처리(`stub.failure-rate=0`로 결정론적 성공) → COMPLETED |

stub 이미지 클라이언트는 실제 구현체이므로 mock이 필요 없고, `application-test.yml`의 `failure-rate`/`delay` 설정으로 각 티어에 필요한 결정론적 결과를 만든다.

## 9. 미해결 / 향후 과제 (원안 계승)

- 대형 Organization의 트래픽 집중 시 balance row 잠금 경합 → 샤딩 또는 Redis 원자적 카운터 검토 필요
- Outbox relay 자체의 sent 마킹 실패(전송 성공 후 마킹 전 종료) → 재전송 시 컨슈머 측 idempotent 처리 필요
- 실제 PG 연동 Charge 흐름 (현재는 내부 잔액 증가만 구현)
- 정기 배치: ledger 합산 vs balance 대조로 정합성 검증 (구체 주기/알림 정책 미정)
- 최종 환불 단계에서 organization 조건부 UPDATE가 3회 재시도 후에도 실패할 경우의 운영 알림(현재는 ERROR 로그만)
