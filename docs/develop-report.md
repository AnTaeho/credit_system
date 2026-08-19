# 개발 리포트

- 작성일: 2026-08-19
- 기준: `refactor/db-job-queue` / `32180c1`
- 범위: 전체 코드 리뷰 후 정리한 백로그. 로컬 실행 전용 프로젝트라는 전제로 우선순위를 매겼다.

## 총평

정산 정확성 설계는 견고하다. hold 트랜잭션이 멱등키·잔액 차감·job·ledger를 한 트랜잭션에 묶어 이중 쓰기 구멍이
구조적으로 없고, 상태 전이 네 개 전부 attemptNo fencing을 건 조건부 UPDATE 한 방이며, 원장이
`초기잔액 + Σledger.amount = balance`로 대사 가능하다. 막힌 곳은 정산 로직이 아니라 그 주변이다.

## 백로그

| # | 항목 | 위치 | 상태 |
|---|---|---|---|
| 1 | `scan()` 루프 예외 격리 없음 | `DeadJobSchedulerTask.scan` | ✅ 완료 (2026-08-19) |
| 2 | FAILED 조회 무페이징 + `process()`의 중복 `findById` | `DeadJobSchedulerTask` | ✅ 완료 (2026-08-19) |
| 3 | 재시도 백오프 없음 (즉시 HOLDING 복귀) | `JobLifecycleService.retry` | 하지 않음 — 사용자 판단 |
| 4 | `organizationId` 인덱스 없음 | `JobRepository`, `LedgerRepository` | 하지 않음 — 사용자 판단 |
| 5 | heartbeat 스케줄러 단일 스레드 + Redis 커맨드 타임아웃 미설정 | `HeartbeatRegistry`, `application.yml` | ✅ 완료 (2026-08-19) |
| 6 | 존재하지 않는 조직 ID → 500 | `OrganizationApiController`, `HoldService`, `ChargeService` | ✅ 완료 (2026-08-19) |
| 7 | 원장 대사 배치 (`초기잔액 + Σledger = balance` 검증) | `LedgerReconciliationTask` | ✅ 완료 (2026-08-19) |
| 8 | 멱등키 무한 증가 (TTL·정리 배치 없음) | `idempotency_keys` | **다음 작업** |
| 9 | 다중 인스턴스 폴링 경합 (`SKIP LOCKED` 미적용) | `GenerationWorker.processPendingJobs` | 로컬 단일 인스턴스라 해당 없음 |
| 10 | DB 비밀번호 평문 · `ddl-auto: update` · 관측 수단 없음 | `application.yml` | **보류 — 로컬 전용, 의도된 선택** |
| 11 | 인증 없음 (`X-Organization-Id` 신뢰, `charge`에 결제 없음) | 컨트롤러 4곳 | **보류 — 범위 밖으로 뺀 기능** |

## 완료: `scan()` 예외 격리 (2026-08-19)

**문제였던 것**: `DeadJobSchedulerTask.scan()`의 세 구간(heartbeat 만료 루프 / `reapStaleProcessing` /
FAILED 처리 루프)에 try/catch가 하나도 없었다. job 하나에서 예외가 나면 `scan()` 전체가 중단되고,
id 순서상 뒤에 있던 job은 그 주기에 처리되지 않았다.

- `finalRefund`는 조직 행이 없으면 `IllegalStateException`을 던진다. `ServiceTransactionRollbackTest`가
  실제로 검증하는 경로다.
- 더 현실적인 유입은 일시적 DB 오류(lock wait timeout, deadlock)다.
- 원인이 지속되면 매 주기 같은 자리에서 멈추므로 그 뒤 job은 **영구히 환불되지 않는다.** "환불 유실을
  허용하지 않는다"는 불변식이 깨지는 경로다.

`GenerationWorker.processPendingJobs`는 이미 per-job try/catch + `continue`로 이 문제를 해결해 뒀고
`한_작업의_선점_실패가_같은_배치의_나머지_작업을_막지_않는다` 테스트도 있다. 스케줄러에 같은 규칙을
적용했다.

**적용한 것**: `scan()`을 세 단계 메서드로 나눠 단계마다 try/catch로 감싸고(한 단계가 통째로 실패해도
나머지 단계는 돈다), 각 단계 안에서 다시 항목마다 try/catch + 다음 항목으로 진행하게 했다.
`reapStaleProcessing`의 루프 본문은 `reapIfNoHeartbeat(Job)`으로 추출했고 로직·로그는 그대로다.
회귀 테스트 3건을 추가했다(환불 실패 격리 / heartbeat 만료 회수 실패 격리 / 단계 실패 격리).
전체 테스트 100건 → 103건.

이제 페이징(2번)을 안전하게 넣을 수 있다. 예외 격리가 없는 상태에서 페이징을 먼저 넣었다면 예외를
던지는 job이 첫 페이지를 계속 점유했을 것이다.

## 완료: 스캔 조회 페이징 + 재조회 제거 (2026-08-19)

`processFailedJobs()`와 `reapStaleProcessing()`의 조회에 `PageRequest.of(0, 100)` 상한을 걸었다. 정상
상태에서는 매 주기 배출되므로 폭증하지 않지만, 워커가 오래 죽었다 살아난 뒤의 버스트를 방어한다.
배치 크기는 설정이 아니라 `SCAN_BATCH_SIZE` 클래스 상수다 — 배포마다 조절할 튜닝 노브가 아니라
방어적 상한이라고 판단했다.

`process()`의 `findById` 재조회를 없애고 스냅샷으로 바로 판단하게 했다. 재조회는 이 프로젝트가 쓰지
않겠다고 선언한 check-then-act였고, `retry`/`finalRefund`가 `(id, status=FAILED, attemptNo)` 조건부
UPDATE로 fencing하므로 낡은 스냅샷은 0행으로 무시되고 다음 주기에 자기 교정된다. attemptNo는 재시도마다
증가하고 REFUNDED는 종결이라 ABA 문제도 없다. `markExpiredAsFailed`의 `findById`는 남겼다 — 거기선
heartbeat에서 id만 받아 attemptNo를 모른다.

부수적으로 `findByStatusOrderByIdAsc(JobStatus)`와 `findByStatusAndUpdatedAtBeforeOrderByIdAsc(JobStatus,
Instant)` 무페이징 버전을 제거했다. 후자는 `JobRepositoryTest`만 쓰고 있어 그 테스트를 Pageable 버전으로
옮겼다. 전체 테스트 103건 → 105건.

## 알려진 트레이드오프 (수정 대상 아님)

confirm 재시도 3회를 소진하면 job은 PROCESSING에 남았다가 timeout 회수 후 재시도된다. 이때 이미지 생성은
이미 성공한 상태이므로 외부 API가 두 번 호출된다. 크레딧 정확성은 유지되지만 외부 원가는 이중으로 든다.
at-least-once를 택한 결과다.

## 문서-코드 불일치 (잔여)

- README 4절 "DB unique 제약 위반을 중복 판정으로 사용 **(SELECT 후 INSERT 아님)**" — 실제 `HoldService`는
  SELECT 후 INSERT이고 unique 제약은 경합 시 백스톱으로만 쓴다. 부작용으로 경합 구간에서 재시도한
  클라이언트가 원래 jobId(200 + duplicate) 대신 409를 받는다. `HoldService.toDuplicateResult`의
  `jobId == null` 분기는 전체가 한 트랜잭션이라 사실상 도달 불가능하다.
- 존재하지 않는 `credit_system_design.md` 참조와 같은 줄의 "(Blocker ① 해결, 아래 5절 참고)" 빈 참조.
- 재시도 조건을 `attempt_no < 3`으로 서술 — 실제는 `attemptNo + 1 < maxAttempts`.
- 5절 데이터 모델에 `prompt`, `result_url` 컬럼 누락.
- 기술 스택의 `Testcontainers(MySQL, Redis)` — `build.gradle`에 `testcontainers-mysql`만 있다.
