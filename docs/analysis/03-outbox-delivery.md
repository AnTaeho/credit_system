# Outbox 전송 미보장 → HOLDING 영구 정체 (Blocker) — `42ccb4d`

상태 요약: [docs/PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md)

### 배경
job 생성(HOLD) 커밋과 동시에 outbox 테이블에 발행 대기 레코드를 남기고,
`OutboxRelay.relay()`(1초 주기)가 이를 Kafka로 발행한다. 발행돼야 컨슈머가 job을
PROCESSING으로 가져가 처리한다.

### P — Problem
발행이 보장되지 않으면 job이 HOLDING 상태로 영구히 멈출 수 있다. 특히 순서를 잘못
잡으면(ack 확인 전에 markSent) 전송이 실제로 실패했는데 outbox는 `sent=true`로 남아
**메시지가 영구 유실**된다.

### A — Analysis
- outbox 패턴의 핵심은 "**전송이 확인되기 전엔 발행 대기 상태를 지우지 않는다**"이다.
- ack를 기다리지 않고 `send()` 직후 markSent 하면, 브로커가 실제로 못 받았을 때
  재발행 근거(미전송 레코드)가 사라진다 → HOLDING 정체.
- 그렇다고 relay를 DB 트랜잭션 안에 두면 블로킹 Kafka I/O가 커넥션을 오래 점유한다.

### Action
1. **ack 확인 후에만 markSent** — `send(...).get(10, SECONDS)`로 브로커 ack를 동기 대기한
   뒤 `markSent`. 실패/타임아웃이면 markSent 하지 않고 `break`(다음 주기 재시도).
   `relay()` 자체는 트랜잭션 없이 두고 `markSent`만 리포지토리 트랜잭션으로 커밋해
   블로킹 I/O를 트랜잭션 밖에 유지.
2. **중복은 컨슈머가 흡수** — ack 후 markSent가 지연/실패해 재발행되더라도 컨슈머의
   attemptNo fencing이 중복을 무효화 → "**최소 1회 전송(at-least-once)**"을 택함.
3. **안전망 — `reapStaleHolding()`** — `app.holding.timeout-seconds` 이상 갱신되지 않은
   HOLDING job을 `transitionIfStatusAndAttemptMatch`로 FAILED 전이시켜 기존 재시도/환불
   루프에 연결. 그 사이 outbox가 뒤늦게 발행돼 컨슈머가 이미 PROCESSING으로 가져갔다면
   status+attemptNo 이중 fencing으로 이 전이는 0행이 되어 무효.

### R — Result
- 발행 실패 시 outbox 레코드가 미전송으로 남아 다음 주기에 재시도 → 메시지 유실 차단.
- 어떤 실패 조합에서도 HOLDING이 영구 정체되지 않는다(relay 재시도 + reaper 회수).
- at-least-once의 부작용(중복 전송)은 컨슈머 멱등성으로 흡수.
