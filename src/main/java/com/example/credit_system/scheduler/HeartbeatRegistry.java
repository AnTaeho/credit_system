package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.config.WorkerProperties;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HeartbeatRegistry {

    private static final String KEY = "heartbeats";

    private static final Instant NONE = Instant.EPOCH;

    private final StringRedisTemplate redisTemplate;
    private final AppProperties appProperties;
    private final ScheduledExecutorService executor;
    private final Clock clock;

    private final AtomicReference<Instant> lastRedisFailureAt = new AtomicReference<>(NONE);

    private final AtomicReference<Instant> suppressionStartedAt = new AtomicReference<>(NONE);

    private final AtomicReference<Instant> lastSuppressionAlertAt = new AtomicReference<>(NONE);

    @Autowired
    public HeartbeatRegistry(StringRedisTemplate redisTemplate,
                             AppProperties appProperties,
                             WorkerProperties workerProperties) {
        this(redisTemplate, appProperties, workerProperties, Clock.systemUTC());
    }

    HeartbeatRegistry(StringRedisTemplate redisTemplate,
                      AppProperties appProperties,
                      WorkerProperties workerProperties,
                      Clock clock) {
        this.redisTemplate = redisTemplate;
        this.appProperties = appProperties;
        this.executor = Executors.newScheduledThreadPool(workerProperties.concurrency());
        this.clock = clock;
    }

    public ScheduledFuture<?> startHeartbeat(Long jobId) {
        touch(jobId);
        long interval = appProperties.heartbeat().refreshIntervalSeconds();
        return executor.scheduleAtFixedRate(() -> touch(jobId), interval, interval, TimeUnit.SECONDS);
    }

    public void stopHeartbeat(Long jobId, ScheduledFuture<?> future) {
        future.cancel(false);
        remove(jobId);
    }

    public Set<Long> findExpiredJobIds() {
        if (isInRecoveryGrace()) {
            log.debug("Redis 복구 유예 구간, heartbeat 만료 판정 보류");
            return Set.of();
        }
        double now = clock.instant().getEpochSecond();
        Set<String> expired;
        try {
            expired = redisTemplate.opsForZSet().rangeByScore(KEY, Double.NEGATIVE_INFINITY, now);
        } catch (RuntimeException e) {
            recordRedisFailure();
            log.warn("heartbeat 만료 조회 실패, 이번 주기는 건너뜀", e);
            return Set.of();
        }
        if (expired == null || expired.isEmpty()) {
            return Set.of();
        }
        return expired.stream().map(Long::parseLong).collect(Collectors.toSet());
    }

    private void touch(Long jobId) {
        double expireAt = clock.instant().getEpochSecond() + appProperties.heartbeat().timeoutSeconds();
        try {
            redisTemplate.opsForZSet().add(KEY, jobId.toString(), expireAt);
        } catch (RuntimeException e) {
            recordRedisFailure();
            log.warn("heartbeat 갱신 실패: jobId={}", jobId, e);
        }
    }

    public boolean hasLiveHeartbeat(Long jobId) {
        if (isInRecoveryGrace()) {
            log.debug("Redis 복구 유예 구간, 회수 보류: jobId={}", jobId);
            return true;
        }
        Double score;
        try {
            score = redisTemplate.opsForZSet().score(KEY, jobId.toString());
        } catch (RuntimeException e) {
            recordRedisFailure();
            log.warn("heartbeat 조회 실패, 회수 보류: jobId={}", jobId, e);
            return true;
        }
        return score != null && score > clock.instant().getEpochSecond();
    }

    public void remove(Long jobId) {
        try {
            redisTemplate.opsForZSet().remove(KEY, jobId.toString());
        } catch (RuntimeException e) {
            recordRedisFailure();
            log.warn("heartbeat 제거 실패: jobId={}", jobId, e);
        }
    }

    private void recordRedisFailure() {
        Instant now = clock.instant();
        if (!isInGraceAt(now)) {
            suppressionStartedAt.set(now);
            lastSuppressionAlertAt.set(now);
        }
        lastRedisFailureAt.set(now);
    }

    private boolean isInRecoveryGrace() {
        Instant now = clock.instant();
        if (isInGraceAt(now)) {
            alertIfSuppressionProlonged(now);
            return true;
        }
        clearSuppression(now);
        return false;
    }

    private boolean isInGraceAt(Instant now) {
        return now.isBefore(lastRedisFailureAt.get().plusSeconds(appProperties.heartbeat().timeoutSeconds()));
    }

    private void alertIfSuppressionProlonged(Instant now) {
        long alertSeconds = appProperties.heartbeat().suppressionAlertSeconds();
        Instant startedAt = suppressionStartedAt.get();
        Instant lastAlertAt = lastSuppressionAlertAt.get();
        if (startedAt.equals(NONE) || now.isBefore(lastAlertAt.plusSeconds(alertSeconds))) {
            return;
        }
        if (!lastSuppressionAlertAt.compareAndSet(lastAlertAt, now)) {
            return;
        }
        log.error("Redis 장애가 {}초째 지속 중입니다. PROCESSING job 회수가 그동안 계속 억제되고 있습니다. "
                        + "Redis가 복구될 때까지 만료 job은 FAILED로 전이되지 않고 재시도·환불도 지연됩니다.",
                Duration.between(startedAt, now).getSeconds());
    }

    private void clearSuppression(Instant now) {
        Instant startedAt = suppressionStartedAt.getAndSet(NONE);
        if (startedAt.equals(NONE)) {
            return;
        }
        lastSuppressionAlertAt.set(NONE);
        log.info("Redis 복구 유예 종료, PROCESSING job 회수를 재개합니다: 억제 지속 {}초",
                Duration.between(startedAt, now).getSeconds());
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
