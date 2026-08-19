package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
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

    /** 실패 이력 또는 진행 중인 억제가 없다는 뜻이다. */
    private static final Instant NONE = Instant.EPOCH;

    private final StringRedisTemplate redisTemplate;
    private final AppProperties appProperties;
    private final ScheduledExecutorService executor;
    private final Clock clock;

    // 워커와 스케줄러가 함께 읽고 쓴다. EPOCH는 실패 이력이 없다는 뜻이다.
    private final AtomicReference<Instant> lastRedisFailureAt = new AtomicReference<>(NONE);

    // 유예가 한 번도 풀리지 않고 이어진 구간의 시작 시각이다.
    // lastRedisFailureAt은 유예 중 touch 실패로 계속 재무장되어 장애 지속 시간의 기준이 될 수 없다.
    private final AtomicReference<Instant> suppressionStartedAt = new AtomicReference<>(NONE);

    // 경보를 마지막으로 발행한 시각이다. 판정마다 ERROR가 쏟아지는 것을 막는 재발행 기준점이다.
    private final AtomicReference<Instant> lastSuppressionAlertAt = new AtomicReference<>(NONE);

    /** heartbeat 저장소와 갱신 스레드 풀을 구성한다. */
    @Autowired
    public HeartbeatRegistry(StringRedisTemplate redisTemplate, AppProperties appProperties) {
        this(redisTemplate, appProperties, Clock.systemUTC());
    }

    /** 시각 소스를 주입받는다. 유예 만료를 실제 대기 없이 검증하기 위한 테스트 전용 생성자다. */
    HeartbeatRegistry(StringRedisTemplate redisTemplate, AppProperties appProperties, Clock clock) {
        this.redisTemplate = redisTemplate;
        this.appProperties = appProperties;
        this.executor = Executors.newScheduledThreadPool(1);
        this.clock = clock;
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

    /** 만료된 heartbeat의 작업 ID를 조회한다. Redis 장애와 복구 유예 구간에는 빈 집합을 반환한다. */
    public Set<Long> findExpiredJobIds() {
        if (isInRecoveryGrace()) {
            // 장애 중 touch가 전부 실패해 모든 score가 과거다. 지금 조회하면 살아있는 job까지 만료로 잡힌다.
            log.debug("Redis 복구 유예 구간, heartbeat 만료 판정 보류");
            return Set.of();
        }
        double now = clock.instant().getEpochSecond();
        Set<String> expired;
        try {
            expired = redisTemplate.opsForZSet().rangeByScore(KEY, Double.NEGATIVE_INFINITY, now);
        } catch (RuntimeException e) {
            // heartbeat 만료 회수만 건너뛰고 실패 작업의 재시도·환불 스캔은 계속 진행시킨다.
            recordRedisFailure();
            log.warn("heartbeat 만료 조회 실패, 이번 주기는 건너뜀", e);
            return Set.of();
        }
        if (expired == null || expired.isEmpty()) {
            return Set.of();
        }
        return expired.stream().map(Long::parseLong).collect(Collectors.toSet());
    }

    /**
     * 작업 heartbeat 만료 시각을 갱신한다. Redis 장애 시 삼키고 다음 주기에 회복한다.
     * 회수 판정이 아니라 상태 갱신이므로 유예 구간에도 계속 시도한다. 이게 멈추면 유예가 끝나도 재등록이 안 된다.
     */
    private void touch(Long jobId) {
        double expireAt = clock.instant().getEpochSecond() + appProperties.heartbeat().timeoutSeconds();
        try {
            redisTemplate.opsForZSet().add(KEY, jobId.toString(), expireAt);
        } catch (RuntimeException e) {
            // 예외를 던지면 scheduleAtFixedRate가 이후 갱신을 영구 중단시켜 job이 좀비가 된다.
            recordRedisFailure();
            log.warn("heartbeat 갱신 실패: jobId={}", jobId, e);
        }
    }

    /** 작업에 유효한 heartbeat가 있는지 확인한다. Redis 장애와 복구 유예 구간에는 살아있다고 보수적으로 판정한다. */
    public boolean hasLiveHeartbeat(Long jobId) {
        if (isInRecoveryGrace()) {
            log.debug("Redis 복구 유예 구간, 회수 보류: jobId={}", jobId);
            return true;
        }
        Double score;
        try {
            score = redisTemplate.opsForZSet().score(KEY, jobId.toString());
        } catch (RuntimeException e) {
            // 살아있는 워커를 대거 FAILED로 회수해 외부 생성이 중복 실행되는 것을 막는다.
            recordRedisFailure();
            log.warn("heartbeat 조회 실패, 회수 보류: jobId={}", jobId, e);
            return true;
        }
        return score != null && score > clock.instant().getEpochSecond();
    }

    /**
     * 작업 heartbeat를 제거한다. Redis 장애 시 삼킨다.
     * touch와 마찬가지로 상태 갱신이므로 유예 구간에도 계속 시도한다.
     */
    public void remove(Long jobId) {
        try {
            redisTemplate.opsForZSet().remove(KEY, jobId.toString());
        } catch (RuntimeException e) {
            // 워커의 finally에서 예외가 나도 정상 완료 결과를 실패 처리로 덮어쓰지 않는다.
            recordRedisFailure();
            log.warn("heartbeat 제거 실패: jobId={}", jobId, e);
        }
    }

    /** 마지막 Redis 실패 시각을 기록해 회수 판정 유예를 시작한다. */
    private void recordRedisFailure() {
        Instant now = clock.instant();
        // 유예가 완전히 풀린 뒤의 실패는 새 장애다. 유예 중의 실패는 같은 장애가 이어지는 것이므로
        // 시작 시각을 덮어쓰지 않아야 지속 시간이 재무장에 씻겨나가지 않는다.
        if (!isInGraceAt(now)) {
            suppressionStartedAt.set(now);
            // 경보 기준점을 장애 시작 시각으로 맞춰두면 최초 임계치 도달과 이후 재발행이 같은 식으로 판정된다.
            lastSuppressionAlertAt.set(now);
        }
        lastRedisFailureAt.set(now);
    }

    /**
     * Redis 복구 유예 구간인지 판정하고, 억제가 길어지거나 풀린 사실을 관측 가능하게 남긴다.
     * 유예 길이를 timeoutSeconds로 두면 그 사이 살아있는 워커는 refreshIntervalSeconds 주기로
     * 최소 한 번 touch에 성공해 score를 미래로 밀어놓는다. 유예가 끝나면 진짜 죽은 job만 남는다.
     */
    private boolean isInRecoveryGrace() {
        Instant now = clock.instant();
        if (isInGraceAt(now)) {
            alertIfSuppressionProlonged(now);
            return true;
        }
        clearSuppression(now);
        return false;
    }

    /** 부수효과 없는 유예 판정이다. 실패 기록 경로에서 경보 상태를 건드리지 않고 재사용한다. */
    private boolean isInGraceAt(Instant now) {
        return now.isBefore(lastRedisFailureAt.get().plusSeconds(appProperties.heartbeat().timeoutSeconds()));
    }

    /**
     * 억제가 임계치를 넘겨 지속되면 ERROR로 경보한다.
     * 장애가 길어져도 유예에 상한을 두지 않는 것은 의도된 동작이다. Redis가 죽은 동안에는 살아있는 job과
     * 죽은 job을 구분할 수 없어 회수를 재개하면 정상 워커의 job을 대거 FAILED로 만들기 때문이다.
     * 대신 "회수할 게 없음"과 "회수가 무기한 억제됨"이 로그에서 구분되도록 상태를 드러낸다.
     */
    private void alertIfSuppressionProlonged(Instant now) {
        long alertSeconds = appProperties.heartbeat().suppressionAlertSeconds();
        Instant startedAt = suppressionStartedAt.get();
        Instant lastAlertAt = lastSuppressionAlertAt.get();
        if (startedAt.equals(NONE) || now.isBefore(lastAlertAt.plusSeconds(alertSeconds))) {
            return;
        }
        // 여러 판정이 동시에 임계치를 넘겨도 한 스레드만 발행하게 해 로그 폭주를 막는다.
        if (!lastSuppressionAlertAt.compareAndSet(lastAlertAt, now)) {
            return;
        }
        log.error("Redis 장애가 {}초째 지속 중입니다. PROCESSING job 회수가 그동안 계속 억제되고 있습니다. "
                        + "Redis가 복구될 때까지 만료 job은 FAILED로 전이되지 않고 재시도·환불도 지연됩니다.",
                Duration.between(startedAt, now).getSeconds());
    }

    /** 유예가 풀리면 억제 상태를 리셋하고 회복 사실을 남긴다. */
    private void clearSuppression(Instant now) {
        Instant startedAt = suppressionStartedAt.getAndSet(NONE);
        if (startedAt.equals(NONE)) {
            return;
        }
        lastSuppressionAlertAt.set(NONE);
        log.info("Redis 복구 유예 종료, PROCESSING job 회수를 재개합니다: 억제 지속 {}초",
                Duration.between(startedAt, now).getSeconds());
    }

    /** heartbeat 갱신 스레드 풀을 종료한다. */
    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
