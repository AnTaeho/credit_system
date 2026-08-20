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
        try {
            redisTemplate.opsForZSet().remove(KEY, member);
            log.warn("해석할 수 없는 heartbeat 멤버 제거: {}", member);
        } catch (RuntimeException e) {
            outageGate.recordFailure();
            log.warn("해석할 수 없는 heartbeat 멤버 제거 실패: {}", member, e);
        }
    }

    private void refreshHeartbeat(JobAttempt attempt) {
        double expireAt = clock.instant().getEpochSecond() + appProperties.heartbeat().timeoutSeconds();
        try {
            redisTemplate.opsForZSet().add(KEY, attempt.toMember(), expireAt);
        } catch (RuntimeException e) {
            outageGate.recordFailure();
            log.warn("heartbeat 갱신 실패: jobId={}, attemptNo={}", attempt.jobId(), attempt.attemptNo(), e);
        }
    }

    public boolean hasLiveHeartbeat(Long jobId, int attemptNo) {
        if (outageGate.isInRecoveryGrace()) {
            log.debug("Redis 복구 유예 구간, 회수 보류: jobId={}, attemptNo={}", jobId, attemptNo);
            return true;
        }
        String member = new JobAttempt(jobId, attemptNo).toMember();
        Double score;
        try {
            score = redisTemplate.opsForZSet().score(KEY, member);
        } catch (RuntimeException e) {
            outageGate.recordFailure();
            log.warn("heartbeat 조회 실패, 회수 보류: jobId={}, attemptNo={}", jobId, attemptNo, e);
            return true;
        }
        return score != null && score > clock.instant().getEpochSecond();
    }

    public void removeHeartbeat(Long jobId, int attemptNo) {
        String member = new JobAttempt(jobId, attemptNo).toMember();
        try {
            redisTemplate.opsForZSet().remove(KEY, member);
        } catch (RuntimeException e) {
            outageGate.recordFailure();
            log.warn("heartbeat 제거 실패: jobId={}, attemptNo={}", jobId, attemptNo, e);
        }
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
