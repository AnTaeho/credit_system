# 잔액 차감 전략 3종 부하 테스트 설계

작성일: 2026-08-02

## 배경

이 프로젝트는 `4632f56`에서 version 기반 낙관적 락을 걷어내고 `balance >= :amount`
조건부 UPDATE로 교체했다. 당시 판단 근거는 정성적 분석이었다(REPEATABLE READ 스냅샷
문제, check-then-act 제거). 이 설계는 그 판단을 **처리량과 지연 수치로 뒷받침**한다.

비교 대상은 세 가지다.

| 전략 | 구현 |
|---|---|
| 낙관적 락 | `findById` → `UPDATE ... WHERE id = ? AND version = ?` → 실패 시 새 트랜잭션으로 재시도 |
| 비관적 락 | `SELECT ... FOR UPDATE` → 애플리케이션에서 잔액 검사 → `UPDATE` |
| 조건부 UPDATE | `UPDATE ... WHERE id = ? AND balance >= ?` (현재 프로덕션 방식) |

## 범위

**측정한다**: 잔액 차감 트랜잭션 하나의 처리량(TPS)과 지연(p50/p95/p99), 동시성 레벨별 변화.

**측정하지 않는다**: HTTP 계층, Kafka 발행, Redis heartbeat, job/ledger/outbox 쓰기.
이들은 세 전략에 동일하게 얹히는 상수이므로 배제해야 전략 간 차이가 드러난다.

## 낙관적 락은 올바른 버전으로 구현한다

이 프로젝트가 실제로 가졌던 낙관적 락은 **한 `@Transactional` 안에서** 재시도하는
결함 버전이었다. REPEATABLE READ에서 재읽기가 같은 스냅샷을 반환하므로 재시도가
구조적으로 100% 실패한다. 그대로 벤치마크에 넣으면 TPS 비교가 아니라 "전부 실패"만
나온다.

따라서 벤치마크는 **매 재시도를 `REQUIRES_NEW`로 새 트랜잭션에서 수행**하는 올바른
구현을 사용한다. 세 전략이 모두 제대로 구현된 상태로 비교되어야 수치가 공정하다.

## 구조

```
src/test/java/com/example/credit_system/benchmark/
├── BenchAccount.java              # 벤치 전용 엔티티 (id, balance, version)
├── BenchAccountRepository.java    # 세 전략의 쿼리
├── DeductStrategy.java            # 테스트 소스셋 내 인터페이스
├── OptimisticLockStrategy.java
├── PessimisticLockStrategy.java
├── ConditionalUpdateStrategy.java
├── BenchmarkHarness.java          # 스레드 실행 + 지연 수집 + 백분위 계산
├── BenchmarkResult.java           # record
└── BalanceStrategyBenchmark.java  # @Tag("benchmark") 진입점
```

전략 구현체는 전부 테스트 소스셋에 둔다. 프로덕션 코드는 수정하지 않는다. 이미 결론이
난 설계에 낙관적 락을 다시 집어넣을 이유가 없고, 벤치마크를 지우면 흔적이 완전히
사라진다.

### 벤치 전용 엔티티가 필요한 이유

`Organization`에는 `version` 컬럼이 없다. 낙관적 락을 걷어낼 때 함께 삭제됐다.
낙관적 락 전략에는 필수이므로 `bench_account` 테이블을 따로 만든다. 세 전략이 같은
테이블을 공유하되 비관적/조건부 전략은 `version`을 읽지도 쓰지도 않는다.
`ddl-auto: create-drop`이라 테이블은 자동 생성된다.

### 트랜잭션 경계

- **낙관적 락**: 재시도 루프가 트랜잭션 **밖**에 있고, 각 시도가 `REQUIRES_NEW`로 새
  트랜잭션을 연다. 이래야 스냅샷이 갱신된다.
- **비관적 락 / 조건부 UPDATE**: 단일 `@Transactional`.

## 공정성 장치

- 세 전략 모두 **잔액 차감 하나만** 수행한다. `HoldService`를 쓰지 않는다.
- MySQL 8.4 컨테이너 하나를 `static @Container`로 세 전략이 공유한다.
- 전략마다 **워밍업 라운드**를 돌리고 결과를 버린다 — JIT 컴파일, 커넥션 풀 충전,
  InnoDB 버퍼 풀 적재를 측정에서 배제한다.
- 각 라운드 전에 잔액을 리셋하고, 전략 실행 순서를 고정한다.

## 설정 고정

| 항목 | 값 | 이유 |
|---|---|---|
| HikariCP `maximum-pool-size` | 128 | 최대 동시성 100 초과 확보 |
| MySQL `max_connections` | 200 | 기본 151. 컨테이너 `withCommand`로 상향 |
| 격리 수준 | REPEATABLE READ | MySQL 기본값. 명시해 기록에 남긴다 |
| 차감액 (cost) | 100 | 프로덕션 `app.generation.cost`와 동일 |
| 초기 잔액 | 1,000,000 (= 5000 × 100 × 2) | 전량 성공 보장 |
| 낙관적 락 재시도 상한 | 50회 | 무한 루프 방지. 소진분은 실패로 집계 |

### 커넥션 풀 함정

HikariCP 기본 풀 크기는 10이다. 동시성을 50, 100으로 올리면 스레드 대부분이 커넥션을
기다리고 **그 대기가 락 대기로 오인된다**. 세 전략이 똑같이 왜곡되므로 상대 순위는
살아남지만 절대 수치가 무의미해지고, 특히 **낙관적 락이 부당하게 유리해 보인다** —
재시도할 때마다 커넥션을 반납했다 다시 잡으므로 풀 병목이 재시도 비용을 가린다.
풀 크기를 최대 동시성 이상으로 잡는 것이 이 벤치마크의 전제 조건이다.

## 측정 절차

동시성 `[1, 10, 50, 100]` × 전략 3개 = 12회 실행. 각 실행은 **고정 요청 수 5,000건**을
동시성만큼의 스레드가 나눠 처리한다.

```
ready 배리어 → start 신호(동시 출발) → 요청마다 nanoTime 전후 기록 → done 대기
TPS       = 5000 / wall clock 경과
p50/95/99 = long[5000] 정렬 후 인덱싱
```

수집 지표: **TPS, p50, p95, p99, 성공 수, 실패 수, 낙관적 락 총 재시도 횟수**.

마지막 지표가 핵심이다. 낙관적 락의 지연 증가가 락 대기가 아니라 **재시도 누적** 때문임을
직접 보여준다.

## 실행

`@Tag("benchmark")`로 격리하고 Gradle 태스크를 분리해 기본 `test`에서 제외한다.
12회 실행에 수 분이 걸리므로 CI에서 매번 돌 이유가 없다.

```bash
./gradlew benchmark
```

## 결과물

- 콘솔에 마크다운 표 출력
- `docs/analysis/`에 결과 문서 작성 — 기존 분석 문서와 같은 PARA 형식

## 검증 기준

1. `./gradlew test`가 기존과 동일하게 통과한다 (벤치마크는 태그로 제외됨)
2. `./gradlew benchmark`가 12개 조합을 모두 실행하고 표를 출력한다
3. 모든 전략에서 최종 잔액이 `초기잔액 - 성공수 × cost`와 정확히 일치한다 —
   불일치하면 벤치마크 자체에 결함이 있는 것이다
4. 동시성 1에서는 세 전략의 TPS가 유사해야 한다 (경합이 없으므로).
   여기서 큰 차이가 나면 측정 대상 정렬이 잘못된 것이다
