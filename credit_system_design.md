# Credit System 설계 정리

## 1. 요구사항 요약

- 여러 Organization, 각 Organization에 여러 사용자, 사용자는 소속 Organization의 크레딧 잔액을 공유
- 크레딧은 선결제, 이미지 생성 시 차감, 잔액 부족 시 생성 시작 불가
- 이미지 생성은 비동기, 수십 초 이상 소요
- 생성 실패 시 사용된 크레딧은 환불
- 동일 요청이 네트워크 이슈로 중복 전송될 수 있음
- **제약 조건: 크레딧은 항상 정확하게 차감·환불되어야 함**

## 2. 핵심 설계 원칙

> "확인 후 처리(check-then-act)" 구조를 쓰지 않는다. 모든 상태 변경은 **조건부 UPDATE/INSERT 하나**로 확인과 실행을 원자화한다.

이 원칙 하나가 아래 모든 동시성 문제에 반복적으로 적용된다.

| 문제 상황 | 조건부 연산 |
|---|---|
| 동시 잔액 차감 | `UPDATE organization SET balance=?, version=version+1 WHERE id=? AND version=?` |
| 이중 결과 쓰기(재시도된 워커 vs 원래 워커) | `UPDATE job SET status=? WHERE id=? AND attempt_no=?` |
| 중복 요청 | `INSERT INTO idempotency_keys (...) ` — unique 제약 위반을 중복 판정으로 사용 |
| 최종 refund 시점 늦은 워커의 성공 응답 경합 | `UPDATE job SET status='REFUNDED' WHERE id=? AND status='FAILED' AND attempt_no=?` |
| 스케줄러 다중 인스턴스의 재시도 투입 경합 | `UPDATE job SET attempt_no=attempt_no+1 WHERE id=? AND status='FAILED' AND attempt_no=?` |

## 3. 테이블 구조

**organization**

| 컬럼 | 설명 |
|---|---|
| id (PK) | |
| balance | 잔액. 별도 테이블이 아니라 이 컬럼으로 관리 |
| version | 낙관적 락용 |
| updated_at | |

**job**

| 컬럼 | 설명 |
|---|---|
| id (PK) | |
| org_id (FK) | |
| status | HOLDING / PROCESSING / COMPLETED / FAILED / REFUNDED |
| attempt_no | fencing token, 재시도 투입 시 증가 |
| hold_amount | |
| updated_at | heartbeat |

**idempotency_keys**

| 컬럼 | 설명 |
|---|---|
| id (PK) | |
| org_id (FK) | |
| idem_key | org_id와 함께 unique 제약 |
| job_id (FK) | 자체 status 없음, job.status 참조 |

**ledger**

| 컬럼 | 설명 |
|---|---|
| id (PK) | |
| org_id (FK) | |
| job_id (FK) | |
| type | HOLD / CONFIRM / REFUND / CHARGE |
| amount | |
| created_at | insert-only, 수정 없음 |

**outbox**

| 컬럼 | 설명 |
|---|---|
| id (PK) | |
| job_id (FK) | |
| payload | |
| sent | |
| created_at | |

관계: `organization 1—N job`, `job 1—0/1 idempotency_keys`, `job 1—N ledger`, `job 1—0/1 outbox`

## 4. 흐름별 설계

### 4.1 요청 접수 (Hold)

순서가 중요하다 — 실패 가능성이 높고 비용이 싼 검증부터 앞에 배치, balance 확정을 가능한 한 앞으로 당겨 락 보유/버전 충돌 구간을 최소화한다.

```
BEGIN TRANSACTION
  1. INSERT idempotency_keys (org_id, idem_key)   -- unique 위반 시 즉시 ROLLBACK, 중복 응답
  2. SELECT balance, version FROM organization WHERE id=?
     IF balance < hold_amount: ROLLBACK, 잔액 부족 응답
  3. UPDATE organization SET balance=balance-hold_amount, version=version+1
     WHERE id=? AND version=?                     -- 0 rows면 재시도(최대 N회) 또는 실패
  4. INSERT job (status='HOLDING', attempt_no=0, hold_amount=?)
  5. UPDATE idempotency_keys SET job_id=?
  6. INSERT ledger (type='HOLD', amount=-hold_amount, job_id=?)
  7. INSERT outbox (job_id=?, payload=?, sent=false)
COMMIT
```

이후 별도 relay 프로세스가 outbox를 폴링해 메시지 큐로 전송한다 (DB와 메시지 큐 간 불일치 방지, Outbox 패턴).

### 4.2 워커 처리

- 워커는 큐 메시지에서 `job_id`, `attempt_no`를 받아 로컬에 보관 (DB 재조회 안 함)
- heartbeat는 DB가 아닌 Redis에서 관리 (job 수 증가 시 DB 쓰기 부하 회피)
- 완료 시 자신이 받은 attempt_no를 그대로 결과 쓰기 조건에 사용

**Heartbeat 구현**

- 자료구조: Redis sorted set (`heartbeats`), member=`job_id`, score=`now + timeout`
- 워커가 처리를 시작하는 시점(job consume 시점)에 최초 등록: `ZADD heartbeats {jobId} {now+timeout}`, 동시에 `job.status='PROCESSING'`으로 전이
  - hold 시점(outbox insert)에 미리 등록하지 않는 이유: 큐 대기 시간이 가변적이라 timeout 산정이 불안정해짐
  - `status='HOLDING'`(아직 워커가 안 집은 상태)인 job은 스케줄러 감시 대상에서 제외 — 첫 heartbeat 이전 공백을 죽음으로 오판하는 것 방지
- 처리 중 공유 스케줄러(스레드 풀 고정, job마다 스레드/스케줄러 개별 생성 안 함)가 주기적으로 score 갱신
- 스케줄러(감시자)는 폴링 방식으로 마감 지난 job만 조회: `ZRANGEBYSCORE heartbeats -inf {now}` — job 수와 무관하게 쿼리 1번(O(1))
  - Redis Keyspace Notification(pub/sub) 대안도 검토했으나 at-most-once라 이벤트 유실 위험 있음, 폴링+zset은 유실 없이 같은 효과를 냄 → 채택 안 함
- 완료/실패 처리 시 zset에서 해당 job 제거

### 4.3 완료 처리 (Confirm)

```
BEGIN TRANSACTION
  1. UPDATE job SET status='COMPLETED' WHERE id=? AND attempt_no=?
     -- 0 rows면 이미 무효화된(늙은) 시도 → 여기서 즉시 종료, 이후 단계 실행 안 함
  2. organization.balance는 변경 없음 (confirm은 확정 로그만 남김)
  3. INSERT ledger (type='CONFIRM', amount=0, job_id=?)
COMMIT
```

attempt_no 체크를 가장 앞에 두는 이유: 이 체크가 실패하면 이후 balance/ledger 작업 자체가 무의미하고, 늦게 살아 돌아온 워커의 오염된 쓰기를 원천 차단해야 하기 때문.

### 4.4 실패 처리 (재시도 및 최종 Refund)

- 워커가 명시적 실패를 반환하거나, 스케줄러가 heartbeat 타임아웃(예: 5초 주기 heartbeat 기준 10초 이상 미갱신)으로 죽은 작업을 탐지하면 `job.status='FAILED'`로 전이
- 스케줄러가 FAILED 작업을 폴링:

```
IF job.attempt_no < 3:
    UPDATE job SET attempt_no=attempt_no+1, status='PROCESSING'
    WHERE id=? AND status='FAILED' AND attempt_no=?   -- 스케줄러 다중 인스턴스 경합 방지
    → 성공 시 새 attempt_no로 재투입(outbox insert)

ELSE (3회 소진, 최종 실패):
    BEGIN TRANSACTION
      1. UPDATE job SET status='REFUNDED'
         WHERE id=? AND status='FAILED' AND attempt_no=?   -- 0 rows면 늦은 워커가 이미 성공 처리함, 종료
      2. UPDATE organization SET balance=balance+hold_amount, version=version+1
         WHERE id=? AND version=?
      3. INSERT ledger (type='REFUND', amount=+hold_amount, job_id=?)
    COMMIT
```

### 4.5 중복 요청 방지 (Idempotency)

- Idempotency key는 **클라이언트가 요청 생성 시점에 발급**, 재전송 시 동일 키 재사용이 전제
- 서버는 `INSERT idempotency_keys` 시도만으로 판정 (SELECT 후 INSERT 아님 — 원자성 확보)
- 클라이언트가 재전송 시 실수로 새 키를 발급하는 경우는 서버가 감지 불가 — 클라이언트 책임 영역으로 명시

## 5. 미해결 / 향후 과제

- 대형 Organization의 트래픽 집중 시 balance row 잠금 경합 → 샤딩 또는 Redis 원자적 카운터 검토 필요
- Outbox relay 자체의 sent 마킹 실패(전송 성공 후 마킹 전 종료) → 재전송 시 컨슈머 측 idempotent 처리 필요 (다음 논의 예정)
- Charge(충전) 흐름 — 외부 PG 연동 포함, 별도 설계 필요
- 정기 배치: ledger 합산 vs balance 대조로 정합성 검증 (구체 주기/알림 정책 미정)

---

# Java Pseudocode

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
