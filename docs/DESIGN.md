# 설계 문서 (Design)

> **이 문서의 역할**: 아키텍처·도메인 모델·상태 전이·정합성 불변식과 그 **설계 이유(why)**를
> 담는다. "무엇이 문제였고 해결됐나"(상태)는 `PRODUCTION_READINESS.md`, 이슈별 심층 근거는
> `analysis/`로 보낸다. → 경계는 [`docs/README.md`](./README.md) 참고.

## 1. 시스템 개요

여러 Organization이 각각 크레딧 잔액을 공유하는 선불형 이미지 생성 시스템이다. 사용자는
소속 Organization의 크레딧을 소비해 이미지 생성을 요청하고, 생성은 비동기로 처리되며
(수십 초 이상 소요) 완료 시 확정, 실패 시 환불된다. 동일 요청이 네트워크 이슈로
중복 전송될 수 있으므로 idempotency 처리가 필요하다.

**핵심 불변식: 크레딧은 항상 정확히 차감·환불된다.** 이중 차감·이중 환불·환불 유실이 없어야
하며, 경합 상황에서도 잔액이 음수가 되거나 부정확해져서는 안 된다. 이 불변식을 지키기 위한
설계 원칙과 구체적 장치는 §4에서 다룬다.

## 2. 아키텍처

**컴포넌트**

- **API 서버** — 생성 요청을 받아 idempotency 체크, 잔액 차감, job 생성을 하나의 트랜잭션으로
  처리한다(`HoldService`). 실제 큐 전송은 하지 않고 outbox 테이블에 발행 대기 레코드만 남긴다.
- **Outbox relay** (`OutboxRelay`, 1초 주기) — outbox의 미전송 레코드를 폴링해 Kafka로
  발행하고, 브로커 ack 확인 후에만 `sent=true`로 마킹한다. DB 트랜잭션과 메시지 큐 전송을
  분리해(Outbox 패턴) 두 저장소 간 불일치를 막는다.
- **워커 (Kafka 컨슈머)** — 큐 메시지에서 `jobId`, `attemptNo`를 받아 로컬에 보관한 채 처리한다
  (처리 중 DB를 재조회하지 않는다). 처리 시작 시 Redis heartbeat를 등록하고 `PROCESSING`으로
  전이시키며, 완료/실패 시 자신이 받은 `attemptNo`를 조건으로 결과를 쓴다.
- **스케줄러** (`DeadJobSchedulerTask`, 5초 주기) — heartbeat 만료 감지, 정체된 `HOLDING`/
  `PROCESSING` job 회수(reaper), `FAILED` job의 재시도 투입 또는 최종 환불을 담당한다.
- **Redis** — heartbeat(sorted set)만 담당한다. job 개수가 늘어도 DB에 쓰기 부하를 주지 않기
  위해 heartbeat는 DB가 아닌 Redis에 둔다.
- **MySQL(InnoDB)** — organization, job, idempotency_keys, ledger, outbox 테이블. 정합성의
  최종 근거는 항상 DB 트랜잭션과 조건부 UPDATE다(§4).

**요청 흐름**

```
클라이언트 → API 서버(Hold: idempotency insert + 잔액 차감 + job 생성 + outbox insert, 단일 트랜잭션)
                │
                ▼
         Outbox Relay (폴링, ack 후 markSent)
                │
                ▼
         Kafka topic (generation-jobs)
                │
                ▼
         워커(Consumer): PROCESSING 전이 + heartbeat 등록 → 이미지 생성 → Confirm/Fail
                │                                                    │
                ▼                                                    ▼
         (성공) COMPLETED, ledger CONFIRM                    (실패) FAILED
                                                                      │
                                                                      ▼
                                                    스케줄러: 재시도 투입 또는 최종 REFUND
```

heartbeat 만료 감지, `HOLDING`/`PROCESSING` 정체 회수는 스케줄러가 5초 주기로 병행 수행한다
(§4, §5).

## 3. 도메인 모델

**organization**

| 컬럼 | 설명 |
|---|---|
| id (PK, Long) | |
| balance | 잔액. 별도 테이블이 아니라 이 컬럼으로 관리 |
| version | 증가는 계속되지만(감사용) 잔액 정합성의 근거로는 쓰이지 않는다 — §4 참고 |
| updatedAt | |

**job**

| 컬럼 | 설명 |
|---|---|
| id (PK, Long) | |
| organizationId (FK) | |
| status | HOLDING / PROCESSING / COMPLETED / FAILED / REFUNDED |
| attemptNo | fencing token, 재시도 투입 시 증가 |
| holdAmount | |
| updatedAt | heartbeat/정체 감지 기준 시각 |

**idempotency_keys**

| 컬럼 | 설명 |
|---|---|
| id (PK, Long) | |
| organizationId (FK) | |
| idemKey | organizationId와 함께 unique 제약 |
| jobId (FK, nullable) | 자체 status 없음, job.status를 참조 |

**ledger**

| 컬럼 | 설명 |
|---|---|
| id (PK, Long) | |
| organizationId (FK) | |
| jobId (FK) | |
| type | HOLD / CONFIRM / REFUND / CHARGE |
| amount | |
| createdAt | insert-only, 수정 없음 |

**outbox**

| 컬럼 | 설명 |
|---|---|
| id (PK, Long) | |
| jobId (FK) | |
| payload | |
| sent | |
| createdAt | |

**관계**: `organization 1—N job`, `job 1—0/1 idempotency_keys`, `job 1—N ledger`,
`job 1—0/1 outbox`

**Job 상태머신**

```
HOLDING → PROCESSING → COMPLETED
             │
             ▼
           FAILED ──(재시도 소진 전)──▶ PROCESSING (attemptNo 증가, 재투입)
             │
             └──(재시도 소진)──▶ REFUNDED
```

- `HOLDING`: hold 트랜잭션에서 생성 직후. 아직 워커가 소비하지 않은 상태 — 스케줄러의
  heartbeat 감시 대상에서 제외된다(첫 heartbeat 이전 공백을 죽음으로 오판하지 않기 위해).
- `PROCESSING`: 워커가 메시지를 consume하고 heartbeat를 등록한 시점에 전이.
- `COMPLETED`: 생성 성공, confirm 처리 완료. attemptNo 불일치 시(늙은 시도) 전이 자체가
  무효(0행)가 되어 여기 도달하지 않는다.
- `FAILED`: 워커의 명시적 실패, heartbeat 만료 감지, 또는 `HOLDING`/`PROCESSING` 정체 회수
  (reaper)로 전이. 스케줄러가 폴링해 `attemptNo < maxAttempts`면 재시도(`PROCESSING`으로
  attemptNo 증가 후 재투입), 소진했으면 `REFUNDED`로 최종 처리.
- `REFUNDED`: 재시도 소진 후 환불 완료. `finalRefund`가 job 전이와 잔액 환불을 하나의
  트랜잭션으로 묶으므로, 이 상태에 도달했다는 것은 환불도 함께 성사됐다는 뜻이다(§4).

## 4. 정합성·동시성 설계

### 원칙: check-then-act 배제, 조건부 UPDATE로 원자화

"확인 후 처리(check-then-act)" 구조를 쓰지 않는다. 모든 상태 변경은 **조건부 UPDATE/INSERT
하나**로 확인과 실행을 원자화한다.

| 문제 상황 | 조건부 연산 |
|---|---|
| 동시 잔액 차감 | `UPDATE organization SET balance=balance-:amount ... WHERE id=:id AND balance>=:amount` |
| 이중 결과 쓰기(재시도된 워커 vs 원래 워커) | `UPDATE job SET status=? WHERE id=? AND attempt_no=?` |
| 중복 요청 | `INSERT INTO idempotency_keys (...)` — unique 제약 위반을 중복 판정으로 사용 |
| 최종 refund 시점 늦은 워커의 성공 응답 경합 | `UPDATE job SET status='REFUNDED' WHERE id=? AND status='FAILED' AND attempt_no=?` |
| 스케줄러 다중 인스턴스의 재시도 투입 경합 | `UPDATE job SET attempt_no=attempt_no+1 WHERE id=? AND status='FAILED' AND attempt_no=?` |

### 잔액 증감 — 조건부 UPDATE (재시도 루프 없음)

> 초기 설계는 낙관적 락(version)+재시도였으나 REPEATABLE READ 스냅샷 문제로 조건부 UPDATE로
> 전환했다 — 상세: [docs/analysis/01-optimistic-lock-retry.md](./analysis/01-optimistic-lock-retry.md)

현재 코드의 잔액 차감/증액은 각각 **단일 조건부 UPDATE 문장**이며, 실패 시 재시도하지 않는다.

- **차감(hold)**:
  ```sql
  UPDATE Organization o
  SET o.balance = o.balance - :amount, o.version = o.version + 1, o.updatedAt = :now
  WHERE o.id = :id AND o.balance >= :amount
  ```
  0행이면 `InsufficientBalanceException`으로 즉시 실패 응답 — 재시도하지 않는다.
- **증액(충전/환불)**: 무조건 원자적 증가.
  ```sql
  UPDATE Organization o SET o.balance = o.balance + :amount, o.version = o.version + 1, o.updatedAt = :now
  WHERE o.id = :id
  ```

InnoDB에서 UPDATE는 대상 행에 자동으로 배타적 락(X-lock)을 걸고, 락을 잡은 뒤 최신 커밋값을
읽어(잠금 읽기) 조건을 평가한다. 같은 organization 행을 노리는 트랜잭션들은 이 X-lock으로
자동 직렬화되므로, 재시도 루프 없이도 조건 평가가 항상 최신 잔액 기준으로 이뤄져 정확하다.
`BalanceConflictException`과 `MAX_LOCK_RETRIES` 재시도 루프는 이 전환과 함께 제거됐다.
`version` 컬럼은 여전히 증가하지만 더 이상 잔액 정합성 판단의 근거로 쓰이지 않는다.

**finalRefund의 부분 실패 차단**: 재시도 소진 후 최종 환불(`RefundService.finalRefund`)은
`job: FAILED→REFUNDED` 전이와 `organization.balance` 증액을 하나의 트랜잭션으로 묶는다.
증액이 0행이면(organization 소멸 등 비정상 상황) `IllegalStateException`을 던져 전체
트랜잭션을 롤백한다 — 그 결과 앞서 실행된 `REFUNDED` 전이도 함께 무효화되어 job은 `FAILED`로
남고, 다음 스케줄러 scan에서 다시 잡혀 재환불을 시도한다. "job은 REFUNDED로 확정됐는데 잔액은
안 돌아온" 상태(부분 성공)가 만들어지지 않는다.

### 멱등성 — 상태+attemptNo 이중 fencing, idempotency 제약

- **job 결과 쓰기**: 모든 상태 전이는 `WHERE id=? AND attempt_no=?` (필요시 `AND status=?`)
  조건부 UPDATE다. 워커는 메시지에서 받은 `attemptNo`를 그대로 조건에 사용하므로, 재시도로
  attemptNo가 증가한 뒤에는 늦게 살아 돌아온 옛 워커의 쓰기가 0행으로 무효화된다.
- **중복 요청 방지**: idempotency key는 클라이언트가 요청 생성 시점에 발급하고, 서버는
  `(organizationId, idemKey)` unique 제약을 건 INSERT 시도만으로 중복을 판정한다(SELECT 후
  INSERT가 아니다 — 원자성 확보). `HoldService`는 우선 SELECT로 대부분의 중복을 걸러내고,
  극히 드문 동시 경합만 unique 제약 위반으로 트랜잭션 롤백시켜 `GlobalExceptionHandler`가
  번역한다. `attachJobId`(idempotency key에 jobId 연결)의 갱신 행 수가 1이 아니면
  `IllegalStateException`으로 전체 롤백한다(0행 무통과 차단).
- 클라이언트가 재전송 시 실수로 새 키를 발급하는 경우는 서버가 감지할 수 없다 — 클라이언트
  책임 영역이다.

### 신뢰성 장치 — outbox, DLT, reaper

- **Outbox 패턴 (at-least-once)**: hold 트랜잭션은 큐에 직접 쓰지 않고 outbox 테이블에
  발행 대기 레코드만 남긴다. `OutboxRelay`가 폴링해 Kafka로 발행하되, **브로커 ack를 받은
  뒤에만 `markSent`** 한다 — ack 전에 마킹하면 실제 전송 실패 시 재발행 근거가 사라져
  메시지가 영구 유실되기 때문이다. 반대로 ack 후 markSent가 지연/실패해 재발행되더라도
  컨슈머의 attemptNo fencing이 중복을 무효화하므로, "최소 1회 전송(at-least-once)"을
  택했다 — 컨슈머 멱등이 중복을 흡수한다. relay 자체는 블로킹 Kafka I/O를 DB 트랜잭션 밖에
  두기 위해 트랜잭션 없이 실행되고, `markSent`만 리포지토리 트랜잭션으로 커밋된다.
- **Kafka DLT**: 컨슈머에 `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`를 구성해,
  파싱 실패 등 재시도로 해결되지 않는 poison message가 파티션 전체를 막지 않도록 유한 횟수
  시도 후 DLT로 격리한다. DLT로 격리된 메시지의 job이 `PROCESSING`에 남는 문제는 아래
  `reapStaleProcessing`이 회수한다.
- **`DeadJobSchedulerTask`의 reaper**:
  - `reapStaleHolding()`: outbox 발행 실패 등으로 `HOLDING`에서 영구 정체될 수 있는 job을,
    일정 시간 이상 갱신되지 않았으면 `FAILED`로 되돌려 기존 재시도/환불 루프에 연결한다.
    status+attemptNo 이중 fencing 덕에, 그 사이 outbox가 뒤늦게 발행되어 컨슈머가 이미
    `PROCESSING`으로 가져갔다면 이 전이는 0행으로 무효화된다.
  - `reapStaleProcessing()`: heartbeat 만료 감지만으로는 "PROCESSING 진입 직후 heartbeat
    등록 전 워커 크래시", "등록 후 Redis 데이터 유실(단일 장애점)", "DLT 격리로 인한
    PROCESSING 잔류"를 잡을 수 없다. Redis에 의존하지 않는 DB 기반 최종 안전망으로, 일정
    시간 이상 갱신되지 않고 Redis에 살아있는 heartbeat도 없는 `PROCESSING` job만 `FAILED`로
    되돌린다. 오탐(실제로는 살아있는 워커)이 나더라도 이후 재시도가 attemptNo를 올리므로
    옛 워커의 최종 confirm은 attemptNo 불일치로 0행이 되어 무효화된다 — 최악의 경우 재작업만
    발생할 뿐 이중 차감은 없다.

### Heartbeat (Redis)

- 자료구조: Redis sorted set(`heartbeats`), member=`jobId`, score=`now + timeout`.
- 워커가 처리를 시작하는 시점(consume 시점)에 최초 등록하며, 동시에 `job.status='PROCESSING'`
  으로 전이한다 — hold 시점에 미리 등록하지 않는 이유는 큐 대기 시간이 가변적이라 timeout
  산정이 불안정해지기 때문이다. `HOLDING` 상태 job은 이 감시 대상에서 제외된다.
- 처리 중 공유 스레드 풀(스케줄러 풀 크기 = 컨슈머 동시성 = 파티션 수)이 주기적으로 score를
  갱신한다.
- 스케줄러는 `ZRANGEBYSCORE heartbeats -inf now`로 마감 지난 job만 조회한다 — job 수와
  무관하게 쿼리 1번(O(1)). Redis Keyspace Notification(pub/sub) 대안도 검토했으나
  at-most-once라 이벤트 유실 위험이 있어, 유실 없는 폴링+zset 방식을 택했다.
- 완료/실패 처리 시 zset에서 해당 job을 제거한다.

## 5. 주요 설계 결정 (Decision Log)

- **낙관적 락(version+재시도) → 조건부 UPDATE.** 결정: 잔액 증감을 재시도 없는 단일 조건부
  UPDATE로 구현. 대안: version 컬럼 기반 낙관적 락 + 재시도 루프(초기 설계). 이유: REPEATABLE
  READ 하에서 재시도해도 스냅샷이 고정돼 매번 낡은 version을 사용하게 되어 재시도가 구조적으로
  무의미했고, 정당한 요청도 경합만 나면 부당하게 거절됐다. 트레이드오프: 없음에 가깝다 —
  InnoDB 행 락이 같은 organization에 대한 동시 요청을 자동 직렬화하므로 애플리케이션 레벨
  재시도 로직 자체가 불필요해졌다. 상세: [docs/analysis/01-optimistic-lock-retry.md](./analysis/01-optimistic-lock-retry.md)

- **환불 실패를 흡수하지 않고 롤백.** 결정: `finalRefund`에서 잔액 증액이 실패하면
  `IllegalStateException`으로 전체 트랜잭션을 롤백한다. 대안: 로그만 남기고 정상 종료(초기
  구현). 이유: 부분 실패를 커밋하면 job은 REFUNDED로 확정되는데 잔액은 돌아오지 않는 상태가
  영구화되고, REFUNDED가 스케줄러의 FAILED 폴링 대상에서 빠지므로 재처리 기회가 사라진다.
  트레이드오프: organization이 실제로 존재하지 않는 등 회복 불가능한 상황에서도 계속
  FAILED로 남아 스케줄러가 재시도하지만, 크레딧 유실보다 안전한 실패 모드다.

- **Outbox는 브로커 ack 확인 후 markSent(at-least-once).** 결정: relay가 Kafka
  ack를 동기 대기한 뒤에만 sent 마킹, 중복은 컨슈머 attemptNo fencing으로 흡수. 대안: send
  직후 즉시 markSent(더 빠르지만 ack 전 실패 시 유실), 또는 exactly-once 트랜잭션 프로듀서
  (구현 복잡도 증가). 이유: 유실은 크레딧 정합성 불변식을 직접 깨뜨리지만 중복은 컨슈머
  측에서 이미 저렴하게 흡수할 수 있는 장치(attemptNo fencing)가 있었다. 트레이드오프: 드물게
  중복 발행이 발생하지만 정합성에는 영향 없음.

- **Heartbeat는 DB가 아닌 Redis.** 결정: heartbeat를 Redis sorted set으로 관리. 대안: job
  테이블의 updatedAt 컬럼만으로 관리(폴링 시 전체 스캔 필요). 이유: job 수 증가에 따른 DB
  쓰기 부하를 피하고, `ZRANGEBYSCORE`로 O(1) 조회를 유지하기 위해서. 트레이드오프: Redis가
  단일 장애점이 되어 heartbeat 유실 가능성이 생기므로, 이를 보완하는 DB 기반 최종 안전망
  (`reapStaleProcessing`)이 별도로 필요했다.
- **미해결 / 향후 과제**:
  - 대형 Organization의 트래픽 집중 시 balance row 잠금 경합 → 샤딩 또는 Redis 원자적
    카운터 검토 필요.
  - Charge(충전) 흐름 — 외부 PG 연동 포함, 별도 설계 필요.
  - 정기 배치: ledger 합산 vs balance 대조로 정합성 검증(구체 주기/알림 정책 미정).

---

## 부록: 초기 설계 의사코드

※ 초기 설계 스케치. 잔액 동시성(낙관적 락+재시도)은 이후 조건부 UPDATE로 변경됨 — §4 및
docs/analysis/01 참고.

```java
// ===== Entities =====

class Organization {
    UUID id;
    long balance;
    long version;
    Instant updatedAt;
}

class Job {
    UUID id;
    UUID orgId;
    JobStatus status; // HOLDING, PROCESSING, COMPLETED, FAILED, REFUNDED
    int attemptNo;
    long holdAmount;
    Instant updatedAt; // heartbeat
}

class IdempotencyKey {
    UUID id;
    UUID orgId;
    String idemKey;
    UUID jobId; // nullable until job created
}

class LedgerEntry {
    UUID id;
    UUID orgId;
    UUID jobId;
    LedgerType type; // HOLD, CONFIRM, REFUND, CHARGE
    long amount;
    Instant createdAt;
}

class OutboxEntry {
    UUID id;
    UUID jobId;
    String payload;
    boolean sent;
    Instant createdAt;
}


// ===== 4.1 Hold: 요청 접수 =====

class HoldService {

    Result requestGeneration(UUID orgId, String idemKey, long holdAmount, String prompt) {
        return transactionTemplate.execute(() -> {

            // 1) 중복 요청 차단 - INSERT 자체의 unique 제약으로 원자적 판정
            boolean inserted = idempotencyRepo.tryInsert(orgId, idemKey);
            if (!inserted) {
                UUID existingJobId = idempotencyRepo.findJobId(orgId, idemKey);
                return Result.duplicate(existingJobId);
            }

            // 2) 잔액 확인
            Organization org = organizationRepo.find(orgId);
            if (org.balance < holdAmount) {
                throw new RollbackException("잔액 부족");
            }

            // 3) 조건부 UPDATE로 확정 (낙관적 락)
            int updated = organizationRepo.updateBalanceWithVersionCheck(
                orgId, org.balance - holdAmount, org.version
            );
            if (updated == 0) {
                throw new RetryableConflictException(); // 재시도 로직으로 위임
            }

            // 4) job 생성
            Job job = new Job(orgId, JobStatus.HOLDING, /*attemptNo=*/0, holdAmount);
            jobRepo.insert(job);

            // 5) idempotency에 job 연결
            idempotencyRepo.attachJobId(orgId, idemKey, job.id);

            // 6) ledger 기록
            ledgerRepo.insert(orgId, job.id, LedgerType.HOLD, -holdAmount);

            // 7) outbox 기록 (실제 큐 전송은 별도 relay가 담당)
            outboxRepo.insert(job.id, buildPayload(job));

            return Result.success(job.id);
        });
    }
}


// ===== 4.2 Worker =====

class GenerationWorker {

    void process(QueueMessage message) {
        UUID jobId = message.jobId;
        int attemptNo = message.attemptNo; // 메시지에서 받은 스냅샷, DB 재조회 안 함

        startHeartbeat(jobId, attemptNo); // 별도 스레드/스케줄로 주기적 updated_at 갱신

        try {
            GenerationResult result = imageGenerationClient.generate(message.prompt);
            confirmService.confirm(jobId, attemptNo, result);
        } catch (Exception e) {
            failureService.markFailed(jobId, attemptNo);
        } finally {
            stopHeartbeat(jobId);
        }
    }

    void startHeartbeat(UUID jobId, int attemptNo) {
        redis.zadd("heartbeats", now() + TIMEOUT_SECONDS, jobId.toString());
        jobRepo.updateStatusIfAttemptMatches(jobId, JobStatus.PROCESSING, attemptNo);

        sharedScheduler.scheduleAtFixedRate(() -> {
            redis.zadd("heartbeats", now() + TIMEOUT_SECONDS, jobId.toString());
        }, 5, 5, TimeUnit.SECONDS);
    }

    void stopHeartbeat(UUID jobId) {
        redis.zrem("heartbeats", jobId.toString());
    }
}


// ===== 4.3 Confirm =====

class ConfirmService {

    void confirm(UUID jobId, int attemptNo, GenerationResult result) {
        transactionTemplate.execute(() -> {

            // attempt_no 체크를 가장 먼저 - 늙은 워커의 쓰기 차단
            int updated = jobRepo.updateStatusIfAttemptMatches(
                jobId, JobStatus.COMPLETED, attemptNo
            );
            if (updated == 0) {
                return null; // 이미 무효화된 시도, 아무 것도 하지 않고 종료
            }

            Job job = jobRepo.find(jobId);
            ledgerRepo.insert(job.orgId, jobId, LedgerType.CONFIRM, 0);

            return null;
        });
    }
}


// ===== 4.4 Failure / Retry / Final Refund =====

class FailureService {

    static final int MAX_ATTEMPTS = 3;

    void markFailed(UUID jobId, int attemptNo) {
        jobRepo.updateStatusIfAttemptMatches(jobId, JobStatus.FAILED, attemptNo);
    }
}

class DeadJobSchedulerTask {

    static final int HEARTBEAT_TIMEOUT_SECONDS = 10; // heartbeat 주기(5s)의 2배
    static final int MAX_ATTEMPTS = 3;

    // 주기적으로 실행 — Redis ZRANGEBYSCORE로 마감 지난 job만 O(1) 조회
    void scan() {
        Set<String> expiredJobIds = redis.zrangeByScore("heartbeats", -Infinity, now());
        List<Job> deadOrFailed = jobRepo.findByIdsOrStatus(expiredJobIds, JobStatus.FAILED);

        for (Job job : deadOrFailed) {
            if (job.attemptNo < MAX_ATTEMPTS) {
                retry(job);
            } else {
                finalRefund(job);
            }
        }
    }

    void retry(Job job) {
        // WHERE 조건부 UPDATE로 스케줄러 다중 인스턴스 경합 방지
        int updated = jobRepo.incrementAttemptIfMatches(
            job.id, JobStatus.PROCESSING, job.attemptNo
        );
        if (updated == 0) {
            return; // 다른 스케줄러 인스턴스가 이미 처리함
        }
        Job refreshed = jobRepo.find(job.id);
        outboxRepo.insert(job.id, buildPayload(refreshed)); // 새 attempt_no로 재투입
    }

    void finalRefund(Job job) {
        transactionTemplate.execute(() -> {

            // status/attempt_no 동시 체크 - 늦게 성공한 워커와의 경합 방지
            int updated = jobRepo.updateStatusIfStatusAndAttemptMatch(
                job.id, JobStatus.REFUNDED, JobStatus.FAILED, job.attemptNo
            );
            if (updated == 0) {
                return null; // 늦은 워커가 이미 COMPLETED로 바꿔놓음, refund 취소
            }

            Organization org = organizationRepo.find(job.orgId);
            int orgUpdated = organizationRepo.updateBalanceWithVersionCheck(
                job.orgId, org.balance + job.holdAmount, org.version
            );
            if (orgUpdated == 0) {
                throw new RetryableConflictException(); // 재시도 위임
            }

            ledgerRepo.insert(job.orgId, job.id, LedgerType.REFUND, job.holdAmount);
            return null;
        });
    }
}


// ===== Repository 조건부 쿼리 예시 (SQL 매핑) =====

interface OrganizationRepository {
    // UPDATE organization SET balance=?, version=version+1
    // WHERE id=? AND version=?
    int updateBalanceWithVersionCheck(UUID orgId, long newBalance, long expectedVersion);
}

interface JobRepository {
    // UPDATE job SET status=? WHERE id=? AND attempt_no=?
    int updateStatusIfAttemptMatches(UUID jobId, JobStatus status, int attemptNo);

    // UPDATE job SET status=? WHERE id=? AND status=? AND attempt_no=?
    int updateStatusIfStatusAndAttemptMatch(UUID jobId, JobStatus newStatus, JobStatus expectedStatus, int attemptNo);

    // UPDATE job SET attempt_no=attempt_no+1, status=? WHERE id=? AND status='FAILED' AND attempt_no=?
    int incrementAttemptIfMatches(UUID jobId, JobStatus newStatus, int expectedAttemptNo);

    // Redis heartbeat 만료 job_id 목록 + status='FAILED'인 job 조회
    List<Job> findByIdsOrStatus(Set<String> expiredJobIds, JobStatus status);
}

interface IdempotencyKeyRepository {
    // INSERT INTO idempotency_keys(org_id, idem_key) VALUES (?, ?)
    // unique 제약 위반 시 false 리턴 (예외를 잡아서 변환)
    boolean tryInsert(UUID orgId, String idemKey);
}
```
