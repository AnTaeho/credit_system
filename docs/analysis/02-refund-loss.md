# 환불 유실 (Blocker) — `4632f56`

상태 요약: [docs/PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md)

### 배경
생성 job 실패 시 HOLD해 둔 크레딧을 되돌려야 한다. 경로:
`DeadJobSchedulerTask.scan()`(5초 주기) → FAILED job 훑음 → `process(job)` →
재시도 소진(`attemptNo >= maxAttempts`)이면 `refundService.finalRefund()`.

### P — Problem
옛 `finalRefund`는 잔액 반영에 실패해도 **예외 없이 정상 리턴**했다.

```java
// 시작 시 job을 FAILED → REFUNDED 로 전이시킨 상태에서 ...
for (int attempt = 0; attempt < MAX_LOCK_RETRIES; attempt++) {
    int orgUpdated = organizationRepository.addBalance(id, amount, org.getVersion(), ...);
    if (orgUpdated == 1) { ledger 기록; return; }
    log.info("버전 충돌, 재시도...");
}
log.error("환불 잔액 반영 실패 - 운영 알림 필요: ...");   // ← 예외 없이 메서드 종료
```

→ `addBalance` 3회 실패 시 `log.error`만 찍고 정상 종료 → `@Transactional` 정상 커밋 →
**job은 REFUNDED로 확정됐는데 잔액은 한 푼도 안 돌아옴.**

### A — Analysis
- **결함 1 — 부분 실패를 커밋(1번과 같은 뿌리).** 옛 `addBalance`도 `WHERE version=...`이라
  REPEATABLE READ 스냅샷 문제로 경합 시 3회 재시도가 구조적으로 전부 실패할 수 있었다.
  즉 `log.error` 경로는 예외 상황이 아니라 경합만 나면 실제로 밟히는 경로였다.
- **결함 2 — 실패를 삼켜 재처리 기회 소멸.** 커밋으로 job이 REFUNDED로 굳으면 이후
  `scan()`의 FAILED 목록에 안 잡혀 **재환불 경로가 영영 사라진다.** 로그 한 줄 남기고
  크레딧이 영구 증발.

### Action
"부분 실패는 절대 커밋하지 않는다" 원칙 적용.
1. `addBalance`를 무조건 원자적 증가로 교체(1번과 동일) → version 충돌 가짜 실패 제거.
2. 재시도 루프 제거 + 0행이면 `IllegalStateException`으로 **전체 롤백**.

```java
int orgUpdated = organizationRepository.addBalance(job.getOrganizationId(), job.getHoldAmount(), Instant.now());
if (orgUpdated == 1) { ledgerRepository.save(...); return; }
throw new IllegalStateException("환불 잔액 반영 실패: organization이 존재하지 않음, jobId=" + job.getId());
```

### R — Result
- 예외 롤백 시 앞의 `FAILED → REFUNDED` 전이도 함께 무효화 → job은 **FAILED로 유지**.
- 다음 `scan()`에서 다시 잡혀 **새 트랜잭션으로 재환불** → 일시 장애 회복 시 결국 환불 완료.
- "환불 성공 + REFUNDED 확정"이 원자적 한 몸이 됨 → 부분 성공 불가능.
- 멱등성(재환불해도 job은 한 번만 REFUNDED)은 시작부의
  `transitionIfStatusAndAttemptMatch` 조건부 전이가 보장.
