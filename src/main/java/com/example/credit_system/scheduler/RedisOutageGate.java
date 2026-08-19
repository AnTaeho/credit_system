package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class RedisOutageGate {

    private static final Instant NONE = Instant.EPOCH;

    private final AppProperties appProperties;
    private final Clock clock;

    private final AtomicReference<Instant> lastRedisFailureAt = new AtomicReference<>(NONE);

    private final AtomicReference<Instant> suppressionStartedAt = new AtomicReference<>(NONE);

    private final AtomicReference<Instant> lastSuppressionAlertAt = new AtomicReference<>(NONE);

    RedisOutageGate(AppProperties appProperties,
                    Clock clock) {
        this.appProperties = appProperties;
        this.clock = clock;
    }

    public void recordFailure() {
        Instant now = clock.instant();
        if (!isInGraceAt(now)) {
            suppressionStartedAt.set(now);
            lastSuppressionAlertAt.set(now);
        }
        lastRedisFailureAt.set(now);
    }

    public boolean isInRecoveryGrace() {
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
}
