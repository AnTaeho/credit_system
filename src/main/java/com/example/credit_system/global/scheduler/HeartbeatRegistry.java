package com.example.credit_system.global.scheduler;

import com.example.credit_system.global.config.AppProperties;
import jakarta.annotation.PreDestroy;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class HeartbeatRegistry {

    private static final String KEY = "heartbeats";

    private final StringRedisTemplate redisTemplate;
    private final AppProperties appProperties;
    private final ScheduledExecutorService executor;

    /** heartbeat 저장소와 갱신 스레드 풀을 구성한다. */
    public HeartbeatRegistry(StringRedisTemplate redisTemplate, AppProperties appProperties) {
        this.redisTemplate = redisTemplate;
        this.appProperties = appProperties;
        this.executor = Executors.newScheduledThreadPool(appProperties.kafka().partitions());
    }

    /** 작업 heartbeat의 주기적 갱신을 시작한다. */
    public ScheduledFuture<?> startHeartbeat(Long jobId) {
        touch(jobId);
        long interval = appProperties.heartbeat().refreshIntervalSeconds();
        return executor.scheduleAtFixedRate(() -> touch(jobId), interval, interval, TimeUnit.SECONDS);
    }

    /** heartbeat 갱신을 중단하고 저장된 값을 제거한다. */
    public void stopHeartbeat(Long jobId, ScheduledFuture<?> future) {
        future.cancel(false);
        remove(jobId);
    }

    /** 만료된 heartbeat의 작업 ID를 조회한다. */
    public Set<Long> findExpiredJobIds() {
        double now = Instant.now().getEpochSecond();
        Set<String> expired = redisTemplate.opsForZSet().rangeByScore(KEY, Double.NEGATIVE_INFINITY, now);
        if (expired == null || expired.isEmpty()) {
            return Set.of();
        }
        return expired.stream().map(Long::parseLong).collect(Collectors.toSet());
    }

    /** 작업 heartbeat 만료 시각을 갱신한다. */
    private void touch(Long jobId) {
        double expireAt = Instant.now().getEpochSecond() + appProperties.heartbeat().timeoutSeconds();
        redisTemplate.opsForZSet().add(KEY, jobId.toString(), expireAt);
    }

    /** 작업에 유효한 heartbeat가 있는지 확인한다. */
    public boolean hasLiveHeartbeat(Long jobId) {
        Double score = redisTemplate.opsForZSet().score(KEY, jobId.toString());
        return score != null && score > Instant.now().getEpochSecond();
    }

    /** 작업 heartbeat를 제거한다. */
    public void remove(Long jobId) {
        redisTemplate.opsForZSet().remove(KEY, jobId.toString());
    }

    /** heartbeat 갱신 스레드 풀을 종료한다. */
    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
