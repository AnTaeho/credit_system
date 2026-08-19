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
        AtomicInteger recordedCount = new AtomicInteger();
        long[] latenciesMicros = new long[totalRequests];

        int baseShare = totalRequests / concurrency;
        int remainder = totalRequests % concurrency;

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
                    throw new IllegalStateException("Benchmark worker interrupted", e);
                } finally {
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
                        workerFailure.addSuppressed(cause);
                    }
                }
            }
            if (workerFailure != null) {
                throw workerFailure;
            }

            int recorded = recordedCount.get();
            if (recorded != totalRequests) {
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
