package com.example.credit_system.benchmark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(result.successCount()).isEqualTo(40);
    }

    @Test
    @Timeout(30)
    @DisplayName("워커 하나가 터지면 원인 예외를 붙여 실행을 실패시킨다")
    void failsRunAndKeepsCauseWhenOneWorkerThrows() {
        RuntimeException lockTimeout = new RuntimeException("Lock wait timeout exceeded");
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
