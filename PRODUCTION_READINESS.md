# 프로덕션 준비도 검토 (2026-07-05, 2026-07-06 갱신)

전체 코드 검토 결과. 이미지 생성 stub은 검토 대상에서 제외.

## 총평

`check-then-act`를 배제하고 **조건부 UPDATE 하나로 확인+실행을 원자화**하는 원칙이
job 상태전이·confirm·refund·retry 전반에 일관되게 적용돼 있다. attemptNo fencing,
idempotency DB unique 제약, outbox 패턴, heartbeat 기반 dead-job 감지까지 설계가 코드에
잘 반영돼 있고 실 MySQL·Kafka 대상 동시성/E2E 테스트도 갖췄다.

**갱신 (2026-07-06)**: Blocker 3건과 Major 중 신뢰성 관련 2건(컨슈머 에러 처리,
PROCESSING 정체 감지)이 모두 해소됐다. "크레딧은 항상 정확히 차감·환불된다"는 핵심
불변식을 깨는 경로는 더 이상 없다. 남은 항목은 보안·운영 하드닝(CSRF, 마이그레이션,
시크릿 분리)으로, **로컬 데모 전용으로만 운용하기로 결정해 의도적으로 보류**한다.

---

## ✅ 해결됨

### 1. ~~단일 트랜잭션 안의 낙관적 락 재시도 루프~~ (Blocker) — `4632f56`
- 재시도 루프 자체를 제거하고 `findById → version 비교` 대신 **원자적 조건부 UPDATE**로
  대체했다. 차감은 `UPDATE ... SET balance = balance - :amount WHERE id = :id AND
  balance >= :amount` 한 문장으로 확인+실행을 원자화(`OrganizationRepository.deductBalance`),
  증액도 무조건 UPDATE(`addBalance`)로 처리. REPEATABLE READ 스냅샷 문제가 성립할
  여지가 없어졌고, 경합 시 부당 409도 사라졌다.

### 2. ~~환불 유실~~ (Blocker) — `4632f56`
- `RefundService.finalRefund`가 잔액 반영 실패(0행) 시 `log.error` 후 정상 리턴하는 대신
  **`IllegalStateException`을 던져 전체 롤백**한다. job이 REFUNDED로 커밋되지 않고
  FAILED로 남아 다음 scan에서 새 트랜잭션으로 재환불된다. 크레딧 영구 유실 경로 차단.

### 3. ~~Outbox 전송 미보장 → HOLDING 영구 정체~~ (Blocker) — `42ccb4d`
- `OutboxRelay.relay`가 `send().get(10s)`으로 **브로커 ack를 확인한 뒤에만 markSent**.
  ack 후 markSent 실패로 재전송돼도 컨슈머 attemptNo fencing이 중복을 흡수(최소 1회 전송).
- 추가로 `DeadJobSchedulerTask.reapStaleHolding()`이 일정 시간 이상 갱신되지 않은
  HOLDING job을 FAILED로 회수해 기존 재시도/환불 루프에 연결한다.

### 4. ~~Kafka 컨슈머 에러 처리 부재~~ (Major) — `d3cbe73`
- `DefaultErrorHandler`(최초 시도 + 1초 간격 2회 재시도) 후
  `DeadLetterPublishingRecoverer`가 메시지를 `generation-jobs.DLT`로 격리한다
  (`KafkaConsumerConfig`). poison message나 반복 실패 예외가 파티션을 막지 않는다.
- DLT로 격리된 메시지의 job이 PROCESSING에 남는 문제는 아래 5번 reaper가 회수한다.
- 검증: `GenerationWorkerDltTest` — 1파티션 토픽에서 poison 메시지 후속의 정상 메시지가
  처리됨(비블로킹) + poison이 DLT에 실제 수신됨을 단언.

### 5. ~~PROCESSING 진입~heartbeat 등록 사이 크래시 / Redis 단일 장애점~~ (Major) — `d3cbe73`
- Redis heartbeat에 의존하지 않는 **DB 기반 PROCESSING reaper** 추가
  (`DeadJobSchedulerTask.reapStaleProcessing()`): `updatedAt`이
  `app.processing.timeout-seconds`(60s)보다 오래된 PROCESSING job 중 살아있는
  heartbeat(`HeartbeatRegistry.hasLiveHeartbeat`)가 없는 것만 FAILED로 회수.
- 커버 범위: ① PROCESSING 전이~heartbeat 등록 사이 크래시, ② Redis 데이터 유실,
  ③ DLT 격리 후 PROCESSING 정체(4번). 오탐이 나도 status+attemptNo fencing으로 옛
  워커의 confirm이 무효화되므로 안전하다(최악의 경우 재작업, 중복 차감 없음).
- 참고: 컨슈머 프로세스 크래시 자체는 오프셋 미커밋 → Kafka 재배송으로 복구되는 경로가
  이미 있고, reaper는 재배송으로 못 잡는 경우까지 막는 안전망이다.

---

## 🟠 보류 (로컬 데모 전용 운용 결정에 따라 의도적 미수정)

실서비스 배포 시에는 반드시 해결해야 하는 항목들이다.

- **CSRF 방어 없음**: 세션 쿠키 인증인데 Spring Security 미사용. `POST /api/organizations/me/charge`,
  `POST /api/jobs`가 CSRF 토큰 없이 노출.
- **`ddl-auto: update`** (`application.yml`): 프로덕션 위험. Flyway/Liquibase 마이그레이션 전환.
- **DB 비밀번호 하드코딩** (`application.yml`): 로컬 MySQL 계정을 직접 커밋해 사용 중.
  실배포 시 환경변수/시크릿 분리 필수.

---

## 🟡 Minor / 개선

- `spring-boot-h2console` + H2 의존성이 MySQL 앱에 포함 — 프로덕션 빌드에서 제거.
- Kafka 컨슈머 단일 스레드(파티션 1) — 처리량 스케일 및 `HeartbeatRegistry` 스레드풀(2)과
  동시성 상한 정합성 검토.
- ~~`idempotencyKeyRepository.attachJobId` 반환값 무시(`HoldService`) — 0 rows 무통과~~ →
  해결: 갱신 행 수가 1이 아니면 `IllegalStateException`으로 전체 롤백.
- `charge`는 결제 검증 없이 임의 증액 가능 (설계상 PG 연동 유보 — demo 범위 OK).

---

## 잘된 점

- 조건부 UPDATE 원자화 원칙의 일관된 적용, attemptNo·status 이중 체크 정확.
- idempotency에 실제 DB unique 제약이 있어 이중 차감 원천 차단.
- outbox 재배송이 컨슈머 멱등성으로 흡수되는 구조.
- 실 MySQL/Kafka 대상 동시성·E2E 테스트 존재 (DLT 격리 테스트 포함 58건).
