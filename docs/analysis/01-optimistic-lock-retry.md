# 단일 트랜잭션 안의 낙관적 락 재시도 루프 (Blocker) — `4632f56`

상태 요약: [docs/PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md)

### P — Problem
`HoldService`, `ChargeService`, `RefundService`가 한 `@Transactional` 안에서
낙관적 락(version 컬럼) 실패 시 재시도하는 루프를 돌렸다.

```java
for (int attempt = 0; attempt < MAX_LOCK_RETRIES; attempt++) {
    Organization org = organizationRepository.findById(id)...;      // ① 읽기(스냅샷)
    if (org.getBalance() < cost) throw new InsufficientBalanceException(...); // ② 검사
    int updated = organizationRepository.deductBalance(
            id, cost, org.getVersion(), Instant.now());             // ③ version 일치 시만 UPDATE
    if (updated == 1) return ...;
    log.info("버전 충돌, 재시도...");                                  // 실패 → 루프
}
throw new BalanceConflictException(...);                             // 3회 실패 시 409
```

### A — Analysis
이 루프는 **REPEATABLE READ(MySQL InnoDB 기본값)에서 절대 회복하지 못한다.**

- **결함 1 — 재시도가 구조적으로 무의미(스냅샷 문제).**
  REPEATABLE READ는 트랜잭션이 처음 읽은 스냅샷을 끝까지 고정한다. `attempt=0`에서
  읽은 `version`은 `attempt=1`에서 다시 `findById` 해도 **같은 옛 값**을 반환한다.
  반면 UPDATE의 `WHERE version = :expectedVersion`은 **커밋된 최신값** 기준으로 평가된다.
  → 다른 트랜잭션이 이미 version을 올렸다면, 재시도해도 매번 낡은 version을 WHERE에
  넣으므로 **3번 모두 0행이 보장** → 무조건 409. 잔액이 충분한 정당한 요청도 경합만
  나면 부당하게 거절됐다.
- **결함 2 — 애초에 불필요했다.** 잔액 차감의 진짜 불변식은 "잔액이 충분할 때만 차감"이지
  "version이 일치할 때만"이 아니다. version 체크는 목적에 안 맞는 도구였다.

### Action
version 기반 낙관적 락을 걷어내고 **잔액 자체를 조건으로 거는 원자적 조건부 UPDATE**로 교체.

```sql
-- deductBalance: 확인(잔액 충분)과 실행(차감)을 단일 원자적 문장으로
UPDATE Organization o
SET o.balance = o.balance - :amount, o.version = o.version + 1, o.updatedAt = :now
WHERE o.id = :id AND o.balance >= :amount      -- 기존: AND o.version = :expectedVersion
```

- `addBalance`(충전/환불)는 `WHERE o.id = :id`만 남긴 무조건 원자적 증가.
- 호출부에서 `MAX_LOCK_RETRIES` 루프·`expectedVersion` 인자 제거. 차감 0행이면
  `InsufficientBalanceException`으로 전환.
- `BalanceConflictException` 클래스, `GlobalExceptionHandler` 매핑, 관련 테스트 삭제.

### R — Result
- REPEATABLE READ 스냅샷 문제가 성립할 여지 자체가 사라졌다(조건을 DB가 최신값으로 평가).
- 경합 시 부당한 409 제거 — 잔액 충분하면 반드시 성공, 부족할 때만 거절.
- `ConcurrentHoldTest`(실 MySQL) 단언 강화: 잔액 500 / 건당 100 / 10스레드 동시 →
  기존 `success ≤ 5`(느슨) → **정확히 성공 5 + 거절 5 + 최종 잔액 0**.

### 부록 A — 왜 재시도 없이 정확한가 (배타적 락 메커니즘)
InnoDB에서 **데이터를 바꾸는 모든 문장(UPDATE/DELETE/INSERT)은 대상 행에 자동으로
배타적 락(X-lock)을 건다.** `SELECT ... FOR UPDATE` 같은 명시가 필요 없다.

한 UPDATE 문 안에서 일어나는 일:
1. **인덱스 탐색** — `WHERE id = :id`의 `id`는 PK → PK 인덱스로 그 한 행만 정확히 찾음.
2. **락 먼저, 읽기 나중** — 그 행에 X-lock을 먼저 잡은 뒤 **최신 커밋값을 읽는다
   (잠금 읽기 / current read).** 스냅샷을 무시하고 "지금 확정된 값"을 본다.
   이것이 `findById`(스냅샷 읽기)와 갈리는 핵심.
3. **조건 평가** — 최신 balance로 `balance >= amount` 판정. 참이면 갱신, 거짓이면 0행.
4. **커밋/롤백까지 락 유지** — 문장이 끝나도 안 풀린다.

X-lock은 서로 배타적이라 같은 PK 행을 노리는 두 트랜잭션은 **자동으로 직렬화**된다.
다른 조직(다른 행)이면 락이 안 겹쳐 병렬 통과. 그래서 재시도 루프가 불필요하다.

### 부록 B — 동시 요청 시나리오
**잔액 100, 60짜리 요청 2개 동시:**
1. T1이 그 행 X-lock 획득 → `100 >= 60` 참 → balance=40 갱신(커밋 전 락 보유).
2. T2는 같은 행 락 대기(blocked).
3. T1 커밋 → 락 해제. 잔액 40 확정.
4. T2가 락 획득 → **최신값 40을 읽어** `40 >= 60` 거짓 → **0행** → `InsufficientBalanceException`.
→ **T1만 성공, T2는 시작 전 거절, 최종 잔액 40, 음수 없음.**

**잔액 100, 60 + 30 동시:** T1이 60 차감(→40), T2가 최신 40 읽어 `40 >= 30` 참 →
30 차감(→10). **둘 다 성공.** 직렬화는 순서를 강제할 뿐, 가능한 요청까지 막지 않는다.
