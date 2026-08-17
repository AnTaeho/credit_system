package com.example.credit_system.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkHarness {

    private static final long READY_TIMEOUT_SECONDS = 30;
    private static final long DONE_TIMEOUT_MINUTES = 5;

    public static BenchmarkResult run(DeductStrategy strategy, int concurrency, int totalRequests,
                                       long accountId, long amount) {
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        AtomicLong totalRetries = new AtomicLong();
        // 실제로 지연 시간이 기록된 횟수. 워커가 중간에 이탈하면 latenciesMicros에 0이 남아
        // 백분위수가 실제보다 낮게 나오므로, 통계를 내기 전에 이 값으로 완주 여부를 검증한다.
        AtomicInteger recordedCount = new AtomicInteger();
        long[] latenciesMicros = new long[totalRequests];

        int baseShare = totalRequests / concurrency;
        int remainder = totalRequests % concurrency;

        // 워커에서 터진 예외는 Future로만 회수할 수 있다. submit 반환값을 버리면
        // 락 타임아웃 같은 RuntimeException이 스택트레이스 없이 사라지고, 남은 반복이
        // 통째로 건너뛰어진 채 결과만 정상처럼 보고된다.
        List<Future<?>> workers = new ArrayList<>(concurrency);

        int offset = 0;
        for (int t = 0; t < concurrency; t++) {
            int myShare = baseShare + (t < remainder ? 1 : 0);
            int myOffset = offset;
            offset += myShare;

            workers.add(executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int i = 0; i < myShare; i++) {
                        long begin = System.nanoTime();
                        DeductStrategy.DeductOutcome outcome = strategy.deduct(accountId, amount);
                        long end = System.nanoTime();
                        latenciesMicros[myOffset + i] = (end - begin) / 1000L;
                        if (outcome.success()) {
                            successCount.incrementAndGet();
                        } else {
                            failureCount.incrementAndGet();
                        }
                        totalRetries.addAndGet(outcome.retries());
                        recordedCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // 인터럽트도 부분 실행이다. 플래그만 복원하고 정상 종료하면 Future가
                    // 성공으로 남아 오염된 통계가 그대로 보고되므로 예외로 승격시킨다.
                    throw new IllegalStateException("Benchmark worker interrupted", e);
                } finally {
                    // 예외 경로에서도 하네스가 done 래치에서 멈추지 않도록 항상 내린다.
                    done.countDown();
                }
            }));
        }

        try {
            boolean allReady = ready.await(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!allReady) {
                throw new IllegalStateException(
                        "Benchmark threads failed to reach the start barrier within "
                                + READY_TIMEOUT_SECONDS + "s (strategy=" + strategy.name()
                                + ", concurrency=" + concurrency + ")");
            }

            long wallStart = System.nanoTime();
            start.countDown();
            boolean finished = done.await(DONE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            long wallEnd = System.nanoTime();
            executor.shutdown();

            if (!finished) {
                throw new IllegalStateException(
                        "Benchmark run timed out after " + DONE_TIMEOUT_MINUTES
                                + " minutes (strategy=" + strategy.name()
                                + ", concurrency=" + concurrency + ")");
            }

            // done 래치가 모두 내려간 뒤이므로 get()은 사실상 즉시 반환한다
            // (finally의 countDown 직후 FutureTask가 완료 표시되는 찰나만 대기).
            IllegalStateException workerFailure = null;
            for (Future<?> worker : workers) {
                try {
                    worker.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    if (workerFailure == null) {
                        workerFailure = new IllegalStateException(
                                "Benchmark worker failed (strategy=" + strategy.name()
                                        + ", concurrency=" + concurrency + ")", cause);
                    } else {
                        // 여러 워커가 동시에 터지는 게 오히려 흔하다(핫 로우 락 경합).
                        // 첫 번째 원인만 남기지 않도록 나머지를 suppressed로 붙인다.
                        workerFailure.addSuppressed(cause);
                    }
                }
            }
            if (workerFailure != null) {
                throw workerFailure;
            }

            int recorded = recordedCount.get();
            if (recorded != totalRequests) {
                // 예외 없이 반복이 누락된 경우까지 막는 마지막 방어선.
                // 미기록 구간은 0으로 남아 p50/p95/p99를 실제보다 낮게 만든다.
                throw new IllegalStateException(
                        "Benchmark recorded " + recorded + " of " + totalRequests
                                + " operations; latency percentiles would be skewed by unexecuted samples"
                                + " (strategy=" + strategy.name() + ", concurrency=" + concurrency + ")");
            }

            double elapsedSeconds = (wallEnd - wallStart) / 1_000_000_000.0;
            double tps = totalRequests / elapsedSeconds;

            long[] sorted = Arrays.copyOf(latenciesMicros, latenciesMicros.length);
            Arrays.sort(sorted);

            return new BenchmarkResult(
                    strategy.name(),
                    concurrency,
                    tps,
                    percentile(sorted, 0.50),
                    percentile(sorted, 0.95),
                    percentile(sorted, 0.99),
                    successCount.get(),
                    failureCount.get(),
                    totalRetries.get()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Benchmark interrupted", e);
        } finally {
            // 예외로 빠져나가도 스레드 풀이 남지 않도록 한다(정상 경로에서는 이미 shutdown 상태).
            executor.shutdownNow();
        }
    }

    private static long percentile(long[] sorted, double p) {
        if (sorted.length == 0) {
            return 0L;
        }
        int index = (int) Math.ceil(p * sorted.length) - 1;
        index = Math.max(0, Math.min(index, sorted.length - 1));
        return sorted[index];
    }
}
