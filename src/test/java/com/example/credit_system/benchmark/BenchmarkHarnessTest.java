package com.example.credit_system.benchmark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 하네스가 워커 예외를 삼키지 않는지 검증한다.
 *
 * 예외가 묻히면 success + failure < requests가 되고, 실행되지 않은 구간이 0으로 남아
 * 백분위수가 실제보다 낮게 나온다. DB 없이 재현하려고 인메모리 fake 전략을 쓰며,
 * 벤치마크 본체와 달리 "benchmark" 태그를 붙이지 않아 기본 test 태스크에서 돌아간다.
 */
class BenchmarkHarnessTest {

    private static final long ACCOUNT_ID = 1L;
    private static final long AMOUNT = 100L;

    @Test
    @Timeout(30)
    @DisplayName("모든 워커가 완주하면 요청 수만큼 집계된다")
    void aggregatesEveryRequestWhenAllWorkersFinish() {
        InMemoryStrategy strategy = new InMemoryStrategy(10_000L);

        BenchmarkResult result = BenchmarkHarness.run(strategy, 4, 40, ACCOUNT_ID, AMOUNT);

        assertThat(result.successCount() + result.failureCount()).isEqualTo(40);
        assertThat(strategy.invocations()).isEqualTo(40);
        // 잔고 10_000 / 100 = 100회분이므로 40회는 전부 성공해야 한다.
        assertThat(result.successCount()).isEqualTo(40);
    }

    @Test
    @Timeout(30)
    @DisplayName("워커 하나가 터지면 원인 예외를 붙여 실행을 실패시킨다")
    void failsRunAndKeepsCauseWhenOneWorkerThrows() {
        RuntimeException lockTimeout = new RuntimeException("Lock wait timeout exceeded");
        // 첫 호출만 터뜨려 정확히 한 워커만 이탈시킨다.
        InMemoryStrategy strategy = new InMemoryStrategy(10_000L, invocation -> invocation == 1, lockTimeout);

        assertThatThrownBy(() -> BenchmarkHarness.run(strategy, 4, 40, ACCOUNT_ID, AMOUNT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Benchmark worker failed")
                .hasMessageContaining(strategy.name())
                .hasCause(lockTimeout);
    }

    @Test
    @Timeout(30)
    @DisplayName("여러 워커가 터지면 나머지 원인을 suppressed로 보존한다")
    void suppressesAdditionalCausesWhenManyWorkersThrow() {
        RuntimeException lockTimeout = new RuntimeException("Lock wait timeout exceeded");
        InMemoryStrategy strategy = new InMemoryStrategy(10_000L, invocation -> true, lockTimeout);

        Throwable thrown = catchRun(strategy, 4, 40);

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        assertThat(thrown.getCause()).isSameAs(lockTimeout);
        // 워커 4개가 전부 첫 호출에서 터지므로 원인 1 + suppressed 3.
        assertThat(thrown.getSuppressed()).hasSize(3);
        assertThat(thrown.getSuppressed()).allSatisfy(s -> assertThat(s).isSameAs(lockTimeout));
    }

    @Test
    @Timeout(30)
    @DisplayName("워커가 터지면 통계를 반환하지 않는다")
    void neverReportsStatisticsFromPartialRun() {
        RuntimeException boom = new RuntimeException("boom");
        InMemoryStrategy strategy = new InMemoryStrategy(10_000L, invocation -> invocation == 1, boom);

        Throwable thrown = catchRun(strategy, 4, 40);

        // 부분 실행 통계가 정상 결과로 새어 나가면 여기서 thrown이 null이 된다.
        assertThat(thrown).isNotNull();
    }

    private static Throwable catchRun(DeductStrategy strategy, int concurrency, int totalRequests) {
        try {
            BenchmarkHarness.run(strategy, concurrency, totalRequests, ACCOUNT_ID, AMOUNT);
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    /**
     * DB 대신 AtomicLong 잔고를 쓰는 fake. failWhen이 참인 호출에서 지정한 예외를 던져
     * 락 타임아웃 같은 RuntimeException 상황을 재현한다.
     */
    private static final class InMemoryStrategy implements DeductStrategy {

        private final ConcurrentHashMap<Long, AtomicLong> balances = new ConcurrentHashMap<>();
        private final AtomicInteger invocations = new AtomicInteger();
        private final long initialBalance;
        private final FailurePredicate failWhen;
        private final RuntimeException failure;

        InMemoryStrategy(long initialBalance) {
            this(initialBalance, invocation -> false, null);
        }

        InMemoryStrategy(long initialBalance, FailurePredicate failWhen, RuntimeException failure) {
            this.initialBalance = initialBalance;
            this.failWhen = failWhen;
            this.failure = failure;
        }

        @Override
        public String name() {
            return "in-memory-fake";
        }

        @Override
        public DeductOutcome deduct(long accountId, long amount) {
            int invocation = invocations.incrementAndGet();
            if (failure != null && failWhen.shouldFail(invocation)) {
                throw failure;
            }
            AtomicLong balance = balances.computeIfAbsent(accountId, id -> new AtomicLong(initialBalance));
            while (true) {
                long current = balance.get();
                if (current < amount) {
                    return new DeductOutcome(false, 0);
                }
                if (balance.compareAndSet(current, current - amount)) {
                    return new DeductOutcome(true, 0);
                }
            }
        }

        int invocations() {
            return invocations.get();
        }
    }

    @FunctionalInterface
    private interface FailurePredicate {
        boolean shouldFail(int invocation);
    }
}
