package com.example.credit_system.benchmark;

import com.example.credit_system.global.config.WorkerProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.worker.GenerationJobProcessor;
import com.example.credit_system.job.worker.GenerationWorker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

@Tag("benchmark")
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class WorkerBatchSizeBenchmark {

    private static final long DEFAULT_WINDOW_SECONDS = 10;
    private static final String DEFAULT_DURATIONS = "5000,1000,500,200,100,50";
    private static final String DEFAULT_BATCH_SIZES = "1,2,4,8,20";
    private static final int DEFAULT_WORKER_CONCURRENCY = 3;
    private static final long DEFAULT_POLL_MILLIS = 500;

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit")
            .withCommand("--max-connections=200");

    @DynamicPropertySource
    static void datasourceProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.datasource.driver-class-name", mysql::getDriverClassName);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    JobRepository realJobRepository;

    @Test
    void batch_size별_유휴율을_측정한다() {
        long windowSeconds = Long.getLong("bench.window-seconds", DEFAULT_WINDOW_SECONDS);
        long[] durations = parseLongCsv(System.getProperty("bench.durations"), DEFAULT_DURATIONS);
        int[] batchSizes = parseIntCsv(System.getProperty("bench.batch-sizes"), DEFAULT_BATCH_SIZES);
        int workerConcurrency = Integer.getInteger("bench.worker-concurrency", DEFAULT_WORKER_CONCURRENCY);
        long pollMillis = Long.getLong("bench.poll-millis", DEFAULT_POLL_MILLIS);

        runCell(100, 20, workerConcurrency, 2, pollMillis);

        long[] sortedDurationsDesc = Arrays.stream(durations).boxed()
                .sorted((a, b) -> Long.compare(b, a))
                .mapToLong(Long::longValue)
                .toArray();
        int[] sortedBatchSizesAsc = Arrays.stream(batchSizes).sorted().toArray();

        List<BatchSizeResult> results = new ArrayList<>();
        for (long duration : sortedDurationsDesc) {
            for (int batchSize : sortedBatchSizesAsc) {
                results.add(runCell(duration, batchSize, workerConcurrency, windowSeconds, pollMillis));
            }
        }

        printMarkdownTable(results);
    }

    private BatchSizeResult runCell(long durationMillis, int batchSize, int concurrency,
                                     long windowSeconds, long pollMillis) {
        jdbcTemplate.execute("TRUNCATE TABLE jobs");

        int backlogSize = (int) Math.max(20,
                Math.ceil(windowSeconds * concurrency / (durationMillis / 1000.0) * 1.5));
        seedBacklog(backlogSize);

        AtomicLong rowsRead = new AtomicLong();
        AtomicLong claimCount = new AtomicLong();
        AtomicLong rollbackCount = new AtomicLong();
        JobRepository countingRepository = wrapWithCounters(rowsRead, claimCount, rollbackCount);

        AtomicInteger completedCount = new AtomicInteger();
        GenerationJobProcessor fakeProcessor = Mockito.mock(GenerationJobProcessor.class);
        doAnswer(invocation -> {
            Thread.sleep(durationMillis);
            completedCount.incrementAndGet();
            return null;
        }).when(fakeProcessor).runGeneration(any());

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(concurrency);
        executor.setMaxPoolSize(concurrency);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("bench-worker-");
        executor.initialize();

        GenerationWorker worker = new GenerationWorker(countingRepository, fakeProcessor, executor,
                new WorkerProperties(true, batchSize, concurrency), pollMillis);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        ScheduledExecutorService driver = Executors.newSingleThreadScheduledExecutor();
        driver.scheduleWithFixedDelay(() -> {
            try {
                worker.dispatchPendingJobs();
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, 0, pollMillis, TimeUnit.MILLISECONDS);

        long wallStart = System.nanoTime();
        try {
            Thread.sleep(windowSeconds * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        driver.shutdown();
        try {
            driver.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long wallEnd = System.nanoTime();
        int completed = completedCount.get();

        executor.shutdown();
        try {
            executor.getThreadPoolExecutor().awaitTermination(
                    Math.max(5, (durationMillis / 1000) + 5), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (failure.get() != null) {
            throw new IllegalStateException(
                    "드라이버가 dispatchPendingJobs에서 예외를 던졌다 (durationMillis=" + durationMillis
                            + ", batchSize=" + batchSize + ")", failure.get());
        }

        assertThat(completed)
                .as("완료 건수가 0이면 안 된다 (durationMillis=%d, batchSize=%d)", durationMillis, batchSize)
                .isGreaterThan(0);

        double actualWindowSeconds = (wallEnd - wallStart) / 1_000_000_000.0;
        double theoreticalTps = concurrency / (durationMillis / 1000.0);
        double observedTps = completed / actualWindowSeconds;
        double idlePercent = Math.max(0.0, 1.0 - (observedTps / theoreticalTps)) * 100.0;
        double rowsReadPerCompletion = rowsRead.get() / (double) completed;
        double wastedUpdatesPerCompletion = (rollbackCount.get() * 2) / (double) completed;

        return new BatchSizeResult(durationMillis, batchSize, concurrency, theoreticalTps, observedTps,
                idlePercent, rowsReadPerCompletion, wastedUpdatesPerCompletion);
    }

    private void seedBacklog(int count) {
        Instant now = Instant.now();
        List<Object[]> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(new Object[] {1L, "HOLDING", 0, 100L, "bench-prompt", null, now, now});
        }
        jdbcTemplate.batchUpdate("""
                INSERT INTO jobs (organization_id, status, attempt_no, hold_amount, prompt, result_url, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, rows);
    }

    private JobRepository wrapWithCounters(AtomicLong rowsRead, AtomicLong claimCount, AtomicLong rollbackCount) {
        JobRepository countingRepository = Mockito.mock(JobRepository.class,
                AdditionalAnswers.delegatesTo(realJobRepository));

        doAnswer(invocation -> {
            JobStatus status = invocation.getArgument(0);
            Pageable pageable = invocation.getArgument(1);
            List<Job> jobs = realJobRepository.findByStatusOrderByIdAsc(status, pageable);
            rowsRead.addAndGet(jobs.size());
            return jobs;
        }).when(countingRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());

        doAnswer(invocation -> {
            Long jobId = invocation.getArgument(0);
            int attemptNo = invocation.getArgument(1);
            Instant now = invocation.getArgument(2);
            claimCount.incrementAndGet();
            return realJobRepository.startProcessingIfAttemptMatches(jobId, attemptNo, now);
        }).when(countingRepository).startProcessingIfAttemptMatches(any(), anyInt(), any(Instant.class));

        doAnswer(invocation -> {
            Long jobId = invocation.getArgument(0);
            JobStatus newStatus = invocation.getArgument(1);
            JobStatus expectedStatus = invocation.getArgument(2);
            int attemptNo = invocation.getArgument(3);
            Instant now = invocation.getArgument(4);
            if (newStatus == JobStatus.HOLDING) {
                rollbackCount.incrementAndGet();
            }
            return realJobRepository.transitionIfStatusAndAttemptMatch(
                    jobId, newStatus, expectedStatus, attemptNo, now);
        }).when(countingRepository).transitionIfStatusAndAttemptMatch(
                any(), any(), any(), anyInt(), any(Instant.class));

        return countingRepository;
    }

    private static long[] parseLongCsv(String property, String fallback) {
        String value = (property == null || property.isBlank()) ? fallback : property;
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .mapToLong(Long::parseLong)
                .toArray();
    }

    private static int[] parseIntCsv(String property, String fallback) {
        String value = (property == null || property.isBlank()) ? fallback : property;
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    private static void printMarkdownTable(List<BatchSizeResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("| job 소요시간 | batch-size | 이론 TPS | 실측 TPS | 유휴율 % | 완료 1건당 읽은 행 | 완료 1건당 헛돈 UPDATE |\n");
        sb.append("|---|---|---|---|---|---|---|\n");
        for (BatchSizeResult r : results) {
            sb.append(String.format(
                    "| %d | %d | %.1f | %.1f | %.1f | %.1f | %.1f |%n",
                    r.durationMillis(), r.batchSize(), r.theoreticalTps(), r.observedTps(),
                    r.idlePercent(), r.rowsReadPerCompletion(), r.wastedUpdatesPerCompletion()));
        }
        System.out.println(sb);
    }
}
