package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.config.WorkerProperties;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HeartbeatRegistry {

    private static final String KEY = "heartbeats";

    private final StringRedisTemplate redisTemplate;
    private final AppProperties appProperties;
    private final ScheduledExecutorService executor;
    private final Clock clock;
    private final RedisOutageGate outageGate;

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
        this.outageGate = new RedisOutageGate(appProperties, clock);
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
        if (outageGate.isInRecoveryGrace()) {
            log.debug("Redis 복구 유예 구간, heartbeat 만료 판정 보류");
            return Set.of();
        }
        double now = clock.instant().getEpochSecond();
        Set<String> expired;
        try {
            expired = redisTemplate.opsForZSet().rangeByScore(KEY, Double.NEGATIVE_INFINITY, now);
        } catch (RuntimeException e) {
            outageGate.recordFailure();
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
            outageGate.recordFailure();
            log.warn("heartbeat 갱신 실패: jobId={}", jobId, e);
        }
    }

    public boolean hasLiveHeartbeat(Long jobId) {
        if (outageGate.isInRecoveryGrace()) {
            log.debug("Redis 복구 유예 구간, 회수 보류: jobId={}", jobId);
            return true;
        }
        Double score;
        try {
            score = redisTemplate.opsForZSet().score(KEY, jobId.toString());
        } catch (RuntimeException e) {
            outageGate.recordFailure();
            log.warn("heartbeat 조회 실패, 회수 보류: jobId={}", jobId, e);
            return true;
        }
        return score != null && score > clock.instant().getEpochSecond();
    }

    public void remove(Long jobId) {
        try {
            redisTemplate.opsForZSet().remove(KEY, jobId.toString());
        } catch (RuntimeException e) {
            outageGate.recordFailure();
            log.warn("heartbeat 제거 실패: jobId={}", jobId, e);
        }
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
