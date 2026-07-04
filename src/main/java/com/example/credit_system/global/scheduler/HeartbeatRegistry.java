package com.example.credit_system.global.scheduler;

import com.example.credit_system.global.config.AppProperties;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class HeartbeatRegistry {

    private static final String KEY = "heartbeats";

    private final StringRedisTemplate redisTemplate;
    private final AppProperties appProperties;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

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
        double now = Instant.now().getEpochSecond();
        Set<String> expired = redisTemplate.opsForZSet().rangeByScore(KEY, Double.NEGATIVE_INFINITY, now);
        if (expired == null || expired.isEmpty()) {
            return Set.of();
        }
        return expired.stream().map(Long::parseLong).collect(Collectors.toSet());
    }

    private void touch(Long jobId) {
        double expireAt = Instant.now().getEpochSecond() + appProperties.heartbeat().timeoutSeconds();
        redisTemplate.opsForZSet().add(KEY, jobId.toString(), expireAt);
    }

    public void remove(Long jobId) {
        redisTemplate.opsForZSet().remove(KEY, jobId.toString());
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
