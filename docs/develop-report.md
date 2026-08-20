# 개발 리포트

- 작성일: 2026-08-19 / 갱신: 2026-08-20
- 기준: `refactor/db-job-queue` / `33e3c54`
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
| 4 | `organizationId` 인덱스 없음 | `JobRepository`, `LedgerRepository` | 부분 완료 (2026-08-20) — ledger만. `jobs.organization_id`는 목록 API용이라 보류 |
| 5 | heartbeat 스케줄러 단일 스레드 + Redis 커맨드 타임아웃 미설정 | `HeartbeatRegistry`, `application.yml` | ✅ 완료 (2026-08-19) |
| 6 | 존재하지 않는 조직 ID → 500 | `OrganizationApiController`, `HoldService`, `ChargeService` | ✅ 완료 (2026-08-19) |
| 7 | 원장 대사 배치 (`초기잔액 + Σledger = balance` 검증) | `LedgerReconciliationTask` | ✅ 완료 (2026-08-19) |
| 8 | 멱등키 무한 증가 (TTL·정리 배치 없음) | `IdempotencyKeyCleanupTask` | ✅ 완료 (2026-08-19) |
| 9 | 다중 인스턴스 폴링 경합 (`SKIP LOCKED` 미적용) | `GenerationWorker.dispatchPendingJobs` | 로컬 단일 인스턴스라 해당 없음 |
| 10 | DB 비밀번호 평문 · `ddl-auto: update` · 관측 수단 없음 | `application.yml` | **보류 — 로컬 전용, 의도된 선택** |
| 11 | 인증 없음 (`X-Organization-Id` 신뢰, `charge`에 결제 없음) | 컨트롤러 4곳 | **보류 — 범위 밖으로 뺀 기능** |
| 12 | heartbeat 키가 시도를 구분하지 않아 늦은 워커가 살아있는 heartbeat를 지움 | `HeartbeatRegistry` | ✅ 완료 (2026-08-20) |
| 13 | 원장 대사가 매 60초 ledger 전체를 스캔 (offset 페이징이 비용을 못 줄임) | `LedgerReconciliationTask` | ✅ 완료 (2026-08-20) |
| 14 | `@Modifying(clearAutomatically)`에 `flushAutomatically` 없음 | 리포지토리 3곳 | ✅ 완료 (2026-08-20) |
| 15 | `batch-size: 20`의 근거 부재 | `application.yml` | ✅ 완료 (2026-08-20) — 실측 후 3으로 |

## 완료: `scan()` 예외 격리 (2026-08-19)

**문제였던 것**: `DeadJobSchedulerTask.scan()`의 세 구간(heartbeat 만료 루프 / `markStalledJobsAsFailed` /
FAILED 처리 루프)에 try/catch가 하나도 없었다. job 하나에서 예외가 나면 `scan()` 전체가 중단되고,
id 순서상 뒤에 있던 job은 그 주기에 처리되지 않았다.

- `finalRefund`는 조직 행이 없으면 `IllegalStateException`을 던진다. `ServiceTransactionRollbackTest`가
  실제로 검증하는 경로다.
- 더 현실적인 유입은 일시적 DB 오류(lock wait timeout, deadlock)다.
- 원인이 지속되면 매 주기 같은 자리에서 멈추므로 그 뒤 job은 **영구히 환불되지 않는다.** "환불 유실을
  허용하지 않는다"는 불변식이 깨지는 경로다.

`GenerationWorker.dispatchPendingJobs`는 이미 per-job try/catch + `continue`로 이 문제를 해결해 뒀고
`한_작업의_선점_실패가_같은_배치의_나머지_작업을_막지_않는다` 테스트도 있다. 스케줄러에 같은 규칙을
적용했다.

**적용한 것**: `scan()`을 세 단계 메서드로 나눠 단계마다 try/catch로 감싸고(한 단계가 통째로 실패해도
나머지 단계는 돈다), 각 단계 안에서 다시 항목마다 try/catch + 다음 항목으로 진행하게 했다.
항목 로직은 각 단계의 루프 본문에 두고 로직·로그는 바꾸지 않았다.
회귀 테스트 3건을 추가했다(환불 실패 격리 / heartbeat 만료 회수 실패 격리 / 단계 실패 격리).
전체 테스트 100건 → 103건.

이제 페이징(2번)을 안전하게 넣을 수 있다. 예외 격리가 없는 상태에서 페이징을 먼저 넣었다면 예외를
던지는 job이 첫 페이지를 계속 점유했을 것이다.

## 완료: 스캔 조회 페이징 + 재조회 제거 (2026-08-19)

`retryOrRefundFailedJobs()`와 `markStalledJobsAsFailed()`의 조회에 `PageRequest.of(0, 100)` 상한을 걸었다. 정상
상태에서는 매 주기 배출되므로 폭증하지 않지만, 워커가 오래 죽었다 살아난 뒤의 버스트를 방어한다.
배치 크기는 설정이 아니라 `SCAN_BATCH_SIZE` 클래스 상수다 — 배포마다 조절할 튜닝 노브가 아니라
방어적 상한이라고 판단했다.

`retryOrRefundFailedJobs()`의 `findById` 재조회를 없애고 스냅샷으로 바로 판단하게 했다. 재조회는 이 프로젝트가 쓰지
않겠다고 선언한 check-then-act였고, `retry`/`finalRefund`가 `(id, status=FAILED, attemptNo)` 조건부
UPDATE로 fencing하므로 낡은 스냅샷은 0행으로 무시되고 다음 주기에 자기 교정된다. attemptNo는 재시도마다
증가하고 REFUNDED는 종결이라 ABA 문제도 없다. `markExpiredJobsAsFailed`의 `findById`는 이때는 남겼다 —
거기선 heartbeat에서 id만 받아 attemptNo를 몰랐기 때문이다. **2026-08-20에 heartbeat 키가 attemptNo를
싣게 되면서 이 재조회도 사라졌다**(아래 항목 12).

부수적으로 `findByStatusOrderByIdAsc(JobStatus)`와 `findByStatusAndUpdatedAtBeforeOrderByIdAsc(JobStatus,
Instant)` 무페이징 버전을 제거했다. 후자는 `JobRepositoryTest`만 쓰고 있어 그 테스트를 Pageable 버전으로
옮겼다. 전체 테스트 103건 → 105건.

## 2026-08-20 작업 요약

| 항목 | 커밋 | 요지 |
|---|---|---|
| flush 순서 명시 | `e6a6f00` | `@Modifying` 8곳에 `flushAutomatically = true`. 지금 안전한 건 모든 엔티티가 IDENTITY라 `save()`가 INSERT를 즉시 내보내기 때문이고, 이는 id 전략의 성질이지 이 코드의 성질이 아니다. sequence로 바꾸는 순간 멱등키 insert가 조용히 유실된다 |
| 멱등키 정리 인덱스 | `691521d` | `idempotency_keys(created_at)`. 시간당 한 번 도는 정리 배치가 삭제 대상이 없는 주기에도 풀스캔을 냈다 |
| 원장 대사 스캔 제거 | `9653b43` | `ledger_entries(organization_id)` 인덱스 + keyset 페이징. 상세는 아래 |
| heartbeat 시도 격리 | `33e3c54` | zset 멤버를 `jobId` → `jobId:attemptNo`. 상세는 아래 |
| batch-size 실측 | (별도) | `20 → 3`. 상세는 아래 |

## 완료: 원장 대사 스캔 제거 (2026-08-20)

**문제였던 것**: 대사 쿼리가 organizations와 ledger_entries를 LEFT JOIN해 조직별로 GROUP BY하는데,
`ledger_entries.organization_id`에 인덱스가 없어 스키마에서 가장 빨리 자라는 테이블을 통째로 스캔했다.
offset 페이징은 도움이 되지 않았다 — MySQL은 그룹핑을 다 끝낸 뒤에 LIMIT/OFFSET을 적용하므로
`PageRequest.of(page, 100)`은 전송량만 줄였고, 루프가 페이지마다 같은 쿼리를 다시 실행했으므로
실제 비용은 **조직 100명당 전체 스캔 한 번, 매 분**이었다.

**적용한 것**: 인덱스 추가 + `o.id > :lastId` keyset 페이징. 둘 다 필요하다 — 인덱스만으로는 여전히
전체를 집계하고, keyset만으로는 페이지마다 ledger를 스캔한다. `lastId` 갱신은 항목별 try/catch **바깥**에
뒀다. 안에 두면 마지막 행에서 예외가 났을 때 `lastId`가 안 올라 같은 페이지를 영원히 다시 요청한다.

회귀 테스트를 205개 조직 / 불일치 120번째(두 번째 페이지 중간)로 바꿨다. 마지막 페이지 끝에 있으면
중간을 읽든 말든 통과한다.

## 완료: heartbeat 시도 격리 (2026-08-20)

**문제였던 것**: heartbeat zset이 job id만 멤버로 써서 한 job의 모든 시도가 키 하나를 공유했다.
그래서 워커가 자기가 쓰지 않은 heartbeat를 지울 수 있었다 — attempt 0이 정체돼 회수되고, 재시도로
attempt 1이 선점해 heartbeat를 등록한 뒤, 뒤늦게 돌아온 attempt 0의 `finally`가 그걸 지운다.

사고로 안 번진 이유는 refresh 주기(5초)가 정체 판정(60초)보다 훨씬 짧아 다음 refresh가 복구해줬기
때문이다. 무관한 설정 두 개가 우연히 유리하게 정렬돼 있었을 뿐 보장이 아니다.

**적용한 것**: 멤버를 `jobId:attemptNo`로. 해석 불가 멤버(배포 전환기의 옛 형식 등)는 스캔이 만났을 때
zset에서 제거한다. 건너뛰기만 하면 만료 상태로 범위 쿼리에 영원히 걸려 주기마다 warn을 찍는다.

부수 효과로 `markExpiredJobsAsFailed`의 `findById`가 사라졌다. heartbeat가 attemptNo를 싣게 되면서
Redis가 준 값을 조건부 UPDATE에 바로 넣을 수 있고, 유효성 판정은 원래 그 UPDATE가 하고 있었다.

## 완료: batch-size 실측 (2026-08-20)

**재려던 것**: `app.worker.batch-size: 20`의 근거가 어디에도 없었다. concurrency가 3인데 20을 읽는 게
과대 설정인지, job이 빨라질 때를 대비한 보험인지 판단할 데이터가 없었다.

**세웠던 가설 — 틀렸다**: 슬롯이 비는 속도가 `concurrency / job소요시간`이므로 폴러가 뒤처지지 않으려면
`batch-size >= concurrency × (폴링주기 / job소요시간)`이어야 한다고 봤다. 이 식대로면 job이 100ms까지
빨라질 때 batch-size 15가 필요하고, 20은 합리적 보험이 된다.

**측정 구성**: `WorkerBatchSizeBenchmark`(`@Tag("benchmark")`). 실제 `GenerationWorker`, 실제
`ThreadPoolTaskExecutor`(core=max=3, 큐 0), 실제 MySQL을 쓰고 `GenerationJobProcessor`만 D밀리초 점유하는
mock으로 바꿨다. 완료 시 DB 쓰기는 일부러 뺐다 — 처리량에 비례하는 부하가 주지표인 유휴율을 오염시킨다.
`JobRepository`는 `delegatesTo` mock으로 감싸 읽은 행 수·claim·rollback을 셌다. 셀당 윈도우 15초,
폴링 500ms(프로덕션과 같은 fixed-delay), 1차 30셀 + 확인 8셀.

**1차 결과** (concurrency 3, 폴링 500ms)

| job 소요시간 | batch-size | 이론 TPS | 실측 TPS | 유휴율 % | 완료 1건당 읽은 행 | 완료 1건당 헛돈 UPDATE |
|---:|---:|---:|---:|---:|---:|---:|
| 2000ms | 1 | 1.5 | 1.3 | 15.6 | 1.5 | 0.7 |
| 2000ms | 2 | 1.5 | 1.3 | 11.2 | 2.9 | 2.1 |
| 2000ms | 4 | 1.5 | 1.4 | 6.8 | 5.5 | 2.8 |
| 2000ms | 8 | 1.5 | 1.4 | 6.7 | 11.0 | 2.8 |
| 2000ms | 20 | 1.5 | 1.4 | 6.7 | 25.3 | 2.8 |
| 1000ms | 1 | 3.0 | 1.8 | 40.0 | 1.1 | 0.0 |
| 1000ms | 2 | 3.0 | 2.7 | 8.9 | 1.4 | 0.7 |
| 1000ms | 4 | 3.0 | 2.8 | 6.7 | 2.8 | 1.4 |
| 1000ms | 8 | 3.0 | 2.8 | 6.7 | 5.5 | 1.4 |
| 1000ms | 20 | 3.0 | 2.8 | 6.7 | 13.8 | 1.4 |
| 500ms | 1 | 6.0 | 1.9 | 68.9 | 1.0 | 0.0 |
| 500ms | 2 | 6.0 | 3.7 | 37.8 | 1.0 | 0.0 |
| 500ms | 4 | 6.0 | 5.5 | 8.9 | 1.4 | 0.7 |
| 500ms | 8 | 6.0 | 5.6 | 6.7 | 2.8 | 0.7 |
| 500ms | 20 | 6.0 | 5.6 | 6.8 | 6.9 | 0.7 |
| 200ms | 1 | 15.0 | 1.9 | 87.1 | 1.0 | 0.0 |
| 200ms | 2 | 15.0 | 3.9 | 74.2 | 1.0 | 0.0 |
| 200ms | 4 | 15.0 | 5.6 | 62.7 | 1.4 | 0.7 |
| 200ms | 8 | 15.0 | 5.6 | 62.7 | 2.8 | 0.7 |
| 200ms | 20 | 15.0 | 5.6 | 62.7 | 6.9 | 0.7 |
| 100ms | 1 | 30.0 | 1.9 | 93.6 | 1.0 | 0.0 |
| 100ms | 2 | 30.0 | 3.9 | 87.1 | 1.0 | 0.0 |
| 100ms | 4 | 30.0 | 5.6 | 81.3 | 1.4 | 0.7 |
| 100ms | 8 | 30.0 | 5.8 | 80.7 | 2.7 | 0.7 |
| 100ms | 20 | 30.0 | 5.7 | 80.9 | 6.7 | 0.7 |
| 50ms | 1 | 60.0 | 2.0 | 96.7 | 1.0 | 0.0 |
| 50ms | 2 | 60.0 | 3.9 | 93.6 | 1.0 | 0.0 |
| 50ms | 4 | 60.0 | 5.8 | 90.3 | 1.3 | 0.7 |
| 50ms | 8 | 60.0 | 5.8 | 90.3 | 2.7 | 0.7 |
| 50ms | 20 | 60.0 | 5.8 | 90.3 | 6.7 | 0.7 |

**확인 결과** (batch-size 3이 스윕에 없어 추가 측정)

| job 소요시간 | batch-size | 이론 TPS | 실측 TPS | 유휴율 % | 완료 1건당 읽은 행 | 완료 1건당 헛돈 UPDATE |
|---:|---:|---:|---:|---:|---:|---:|
| 1000ms | 2 | 3.0 | 2.7 | 8.9 | 1.4 | 0.7 |
| 1000ms | 3 | 3.0 | 2.8 | 6.7 | 2.1 | 0.7 |
| 1000ms | 4 | 3.0 | 2.8 | 6.9 | 2.8 | 1.4 |
| 1000ms | 5 | 3.0 | 2.8 | 6.8 | 3.5 | 1.4 |
| 200ms | 2 | 15.0 | 3.9 | 74.2 | 1.0 | 0.0 |
| 200ms | 3 | 15.0 | 5.6 | 62.7 | 1.0 | 0.0 |
| 200ms | 4 | 15.0 | 5.6 | 62.7 | 1.3 | 0.7 |
| 200ms | 5 | 15.0 | 5.6 | 62.7 | 1.7 | 0.7 |

**읽히는 것**

실측 TPS가 5.8에서 막힌다. 소요시간을 200ms → 50ms로 4배 빠르게 해도, batch-size를 4 → 20으로 5배
키워도 움직이지 않는다. 5.8은 `concurrency / 폴링주기 = 3 / 0.5초 = 6.0`의 실측값이다. executor 큐가 0이라
한 주기에 concurrency개를 넘기면 그다음은 반드시 거부되고 루프가 `return`한다. **한 폴링 주기가 나눠줄 수
있는 최대치는 batch-size가 아니라 concurrency다.**

따라서 실제 규칙은 이렇다.

> 처리량 상한 = min(concurrency / job소요시간, concurrency / 폴링주기)
>
> batch-size는 이 식에 등장하지 않는다. 필요한 값은 concurrency다.

유휴율 6.7%가 포화 지점이다. 여러 행이 정확히 이 값에 수렴하는데 디스패치 오버헤드와 sleep 정밀도의
바닥값이다. 비용은 batch-size에 선형이다 — 2000ms 행에서 완료 1건당 읽은 행이 1.5 → 2.9 → 5.5 → 11.0 →
25.3으로 간다. batch-size 4에서 20으로 가면 읽는 양이 4.6배가 되고 처리량은 소수점도 안 움직인다.

확인 측정에서 `batch-size = 3`이 4와 처리량이 같으면서 읽는 행이 적고 헛도는 UPDATE가 절반이었다.
200ms 행에서는 헛도는 UPDATE가 정확히 0이다 — 리스트가 concurrency개면 거부 없이 루프가 자연 종료되어
탐침용 왕복 자체가 사라진다. `concurrency + 1`이 아니라 `concurrency`가 답이다.

**적용한 것**: `batch-size: 20 → 3`. README의 두 군데 서술도 맞췄다.

**따라오는 결론**: `batch-size: 20`은 "job이 빨라져도 슬롯이 안 굶게" 하는 보험으로 볼 수 있었지만,
측정 결과 batch-size로는 그 보험이 성립하지 않는다. 그 상황에서 막고 있는 것은 폴링 주기이고 batch-size는
읽기 비용만 더 낸다. job이 빨라질 때 돌려야 할 노브는 **폴링 주기**(또는 concurrency)다. 예를 들어 30 TPS가
필요하면 `폴링주기 <= concurrency / 30 = 100ms`로 내려야 한다.

**측정 범위의 한계**: 이 결론은 **단일 인스턴스** 전제다. 다중 인스턴스에서는 `claim` 실패가 `continue`이므로
남이 선점한 job을 건너뛰고 더 뒤까지 훑을 수 있어야 하고, 그때 batch-size는 처리량이 아니라 경합 헤드룸으로
기능한다. 그 축은 측정하지 않았다(백로그 #9와 같은 이유로 범위 밖). 인스턴스를 늘리면 batch-size 3은
재검토 대상이다.

## 알려진 트레이드오프 (수정 대상 아님)

confirm 재시도 3회를 소진하면 job은 PROCESSING에 남았다가 timeout 회수 후 재시도된다. 이때 이미지 생성은
이미 성공한 상태이므로 외부 API가 두 번 호출된다. 크레딧 정확성은 유지되지만 외부 원가는 이중으로 든다.
at-least-once를 택한 결과다.

## 문서-코드 불일치 (잔여)

- README 4절 "DB unique 제약 위반을 중복 판정으로 사용 **(SELECT 후 INSERT 아님)**" — 실제 `HoldService`는
  SELECT 후 INSERT이고 unique 제약은 경합 시 백스톱으로만 쓴다. 부작용으로 경합 구간에서 재시도한
  클라이언트가 원래 jobId(200 + duplicate) 대신 409를 받는다. `HoldService.resolveDuplicateRequest`의
  `jobId == null` 분기는 전체가 한 트랜잭션이라 사실상 도달 불가능하다.
- 존재하지 않는 `credit_system_design.md` 참조와 같은 줄의 "(Blocker ① 해결, 아래 5절 참고)" 빈 참조.
- 재시도 조건을 `attempt_no < 3`으로 서술 — 실제는 `attemptNo + 1 < maxAttempts`.
- 5절 데이터 모델에 `prompt`, `result_url` 컬럼 누락.
- 기술 스택의 `Testcontainers(MySQL, Redis)` — `build.gradle`에 `testcontainers-mysql`만 있다.
