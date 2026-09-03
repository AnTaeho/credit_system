package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.config.WorkerProperties;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class HeartbeatRegistry {

    private static final String KEY = "heartbeats";

    private final StringRedisTemplate redisTemplate;
    private final AppProperties appProperties;
    private final ScheduledExecutorService executor;
    private final Clock clock;

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

    public ScheduledFuture<?> startHeartbeat(Long jobId, int attemptNo) {
        JobAttempt attempt = new JobAttempt(jobId, attemptNo);
        refreshHeartbeat(attempt);
        long interval = appProperties.heartbeat().refreshIntervalSeconds();
        return executor.scheduleAtFixedRate(() -> refreshHeartbeat(attempt), interval, interval, TimeUnit.SECONDS);
    }

    public void stopHeartbeat(Long jobId, int attemptNo, ScheduledFuture<?> future) {
        future.cancel(false);
        removeHeartbeat(jobId, attemptNo);
    }

    public Set<JobAttempt> findExpiredAttempts() {
        double now = clock.instant().getEpochSecond();
        Set<String> expired = redisTemplate.opsForZSet().rangeByScore(KEY, Double.NEGATIVE_INFINITY, now);
        if (expired == null || expired.isEmpty()) {
            return Set.of();
        }
        Set<JobAttempt> attempts = new HashSet<>();
        for (String member : expired) {
            Optional<JobAttempt> attempt = JobAttempt.parse(member);
            if (attempt.isPresent()) {
                attempts.add(attempt.get());
            } else {
                removeUnparseableMember(member);
            }
        }
        return attempts;
    }

    private void removeUnparseableMember(String member) {
        redisTemplate.opsForZSet().remove(KEY, member);
        log.warn("해석할 수 없는 heartbeat 멤버 제거: {}", member);
    }

    private void refreshHeartbeat(JobAttempt attempt) {
        double expireAt = clock.instant().getEpochSecond() + appProperties.heartbeat().timeoutSeconds();
        try {
            redisTemplate.opsForZSet().add(KEY, attempt.toMember(), expireAt);
        } catch (RuntimeException e) {
            log.warn("heartbeat 갱신 실패: jobId={}, attemptNo={}", attempt.jobId(), attempt.attemptNo(), e);
        }
    }

    public boolean hasLiveHeartbeat(Long jobId, int attemptNo) {
        String member = new JobAttempt(jobId, attemptNo).toMember();
        Double score = redisTemplate.opsForZSet().score(KEY, member);
        return score != null && score > clock.instant().getEpochSecond();
    }

    public void removeHeartbeat(Long jobId, int attemptNo) {
        String member = new JobAttempt(jobId, attemptNo).toMember();
        redisTemplate.opsForZSet().remove(KEY, member);
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
