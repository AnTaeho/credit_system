# 프로덕션 준비도 검토 (2026-07-05)

전체 코드 검토 결과. 이미지 생성 stub은 검토 대상에서 제외.

## 총평

`check-then-act`를 배제하고 **조건부 UPDATE 하나로 확인+실행을 원자화**하는 원칙이
job 상태전이·confirm·refund·retry 전반에 일관되게 적용돼 있다. attemptNo fencing,
idempotency DB unique 제약, outbox 패턴, heartbeat 기반 dead-job 감지까지 설계가 코드에
잘 반영돼 있고 실 MySQL·Kafka 대상 동시성/E2E 테스트도 갖췄다.

**결론: 아직 실사용(프로덕션) 수준은 아니다.** 아래 Blocker가 "크레딧은 항상 정확히
차감·환불된다"는 핵심 불변식을 직접 깬다.

---

## 🔴 Blocker (실사용 전 반드시 수정)

### 1. 단일 트랜잭션 안의 낙관적 락 재시도 루프는 버전 충돌을 복구할 수 없다
- 위치: `HoldService.deductBalanceAndCreateJob`, `RefundService.finalRefund`,
  `ChargeService.charge`
- 문제: 하나의 `@Transactional` 안에서 `findById → 조건부 UPDATE(WHERE version=?)`를
  반복한다. MySQL 기본 격리(REPEATABLE READ)에서 트랜잭션 내 non-locking SELECT는
  시작 시점 스냅샷을 계속 반환하므로, 재시도의 `findById`가 동일한 stale 버전을 읽어
  UPDATE가 매번 0을 리턴 → 3회 모두 실패가 보장된다. 재시도 로직이 사실상 무효.
- 증상: 경합 시 정상 요청이 `BalanceConflictException(409)`로 부당 거절.
  `ConcurrentHoldTest`가 이를 "rejected"로 세고 성공 수를 `<=5`로 느슨하게 단언하는 것이
  증상을 그대로 인정.
- 수정: 재시도는 트랜잭션마다 새 스냅샷을 얻도록. 재시도 루프를 `@Transactional` 밖으로
  빼서 단건 시도만 트랜잭션화하거나, `SELECT ... FOR UPDATE`(비관적 락)로 경합 직렬화.

### 2. 환불 유실 — 핵심 불변식 위반
- 위치: `RefundService.finalRefund`
- 문제: job을 REFUNDED로 전이한 뒤 `addBalance`를 시도하는데, 버전 경합으로 3회 재시도가
  모두 실패하면(위 1번) `log.error`만 남기고 정상 리턴 → 트랜잭션 커밋. 결과: job은
  REFUNDED인데 잔액 미복구 + REFUND 원장 없음. job이 REFUNDED라 스케줄러가 다시 집지
  않아 **크레딧 영구 유실**.
- 수정: 소진 시 `throw`로 전체 롤백(job이 FAILED로 남아 다음 scan에서 새 트랜잭션으로
  재환불). 1번과 병행해야 실제 복구됨.

### 3. Outbox 전송 미보장 → job이 HOLDING에 영구 정체
- 위치: `OutboxRelay.relay`
- 문제: `kafkaTemplate.send(...)`는 비동기인데 ack를 기다리지 않고 즉시 `markSent(true)`.
  전송 실패 시 메시지 유실 → job은 HOLDING인 채 방치. 스케줄러는 PROCESSING(heartbeat
  만료)와 FAILED만 감시하므로 HOLDING 정체 job을 감지·환불하는 경로가 없다.
- 수정: `send().get()`/콜백으로 ack 확인 후에만 markSent. 추가로 오래된 HOLDING job을
  회수하는 reaper 필요.

---

## 🟠 Major

- **Kafka 컨슈머 에러 처리 부재** (`GenerationWorker.consume`): `readValue` 실패(poison
  message)나 `StubGenerationException` 외 예외는 무한 재배송으로 파티션을 막는다.
  DLQ / `DefaultErrorHandler` / 재시도 상한 필요.
- **CSRF 방어 없음**: 세션 쿠키 인증인데 Spring Security 미사용. `POST /api/organizations/me/charge`,
  `POST /api/jobs`가 CSRF 토큰 없이 노출.
- **`ddl-auto: update`** (`application.yml`): 프로덕션 위험. Flyway/Liquibase 마이그레이션 전환.
- **DB 비밀번호 소스 하드코딩** (`application.yml`): 환경변수/시크릿 분리. `.idea`, `.DS_Store`
  추적 여부 점검.
- **PROCESSING 진입~heartbeat 등록 사이 크래시**: heartbeat 미등록 상태로 죽으면 만료
  감지 불가. Redis 유실 시 in-flight job 전부 감시 불가(단일 장애점).

---

## 🟡 Minor / 개선

- `spring-boot-h2console` + H2 의존성이 MySQL 앱에 포함 — 프로덕션 빌드에서 제거.
- Kafka 컨슈머 단일 스레드(파티션 1) — 처리량 스케일 및 `HeartbeatRegistry` 스레드풀(2)과
  동시성 상한 정합성 검토.
- `idempotencyKeyRepository.attachJobId` 반환값 무시(`HoldService`) — 0 rows 무통과.
- `charge`는 결제 검증 없이 임의 증액 가능 (설계상 PG 연동 유보 — demo 범위 OK).

---

## 잘된 점

- 조건부 UPDATE 원자화 원칙의 일관된 적용, attemptNo·status 이중 체크 정확.
- idempotency에 실제 DB unique 제약이 있어 이중 차감 원천 차단.
- outbox 재배송이 컨슈머 멱등성으로 흡수되는 구조.
- 실 MySQL/Kafka 대상 동시성·E2E 테스트 존재.

---

## 권장 처리 순서

1. 재시도 구조를 트랜잭션 경계 밖으로(또는 비관적 락) — Blocker 1
2. 환불 소진 시 롤백 + HOLDING reaper — Blocker 2, 3
3. outbox ack 확인 — Blocker 3
4. 컨슈머 DLQ / CSRF / 마이그레이션 / 시크릿 분리 — Major

1~3 처리 시 실사용 후보 수준 도달.
