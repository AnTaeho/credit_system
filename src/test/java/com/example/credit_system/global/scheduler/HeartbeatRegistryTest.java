package com.example.credit_system.global.scheduler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.credit_system.global.config.AppProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HeartbeatRegistryTest {

    private static final String KEY = "heartbeats";

    @Mock StringRedisTemplate redisTemplate;
    @Mock ZSetOperations<String, String> zSetOperations;

    HeartbeatRegistry registry;
    MutableClock clock;
    Logger registryLogger;
    ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        AppProperties appProperties = new AppProperties(
                new AppProperties.Generation(100L, 3),
                null,
                new AppProperties.Heartbeat(10, 1, 60),
                new AppProperties.Processing(60));
        clock = new MutableClock(Instant.now());
        registry = new HeartbeatRegistry(redisTemplate, appProperties, clock);

        logAppender = new ListAppender<>();
        logAppender.start();
        registryLogger = (Logger) LoggerFactory.getLogger(HeartbeatRegistry.class);
        registryLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        registryLogger.detachAppender(logAppender);
        registry.shutdown();
    }

    @Test
    void touch가_Redis_예외를_삼키고_갱신_스레드를_죽이지_않는다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.add(anyString(), anyString(), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        ScheduledFuture<?> future = registry.startHeartbeat(1L);

        // 첫 동기 touch 1회 + 1초 주기 갱신 2회 이상 = 스케줄이 억제되지 않았다는 증거
        verify(zSetOperations, timeout(5000).atLeast(3)).add(eq(KEY), eq("1"), anyDouble());
        assertThat(future.isDone()).isFalse();
        future.cancel(false);
    }

    @Test
    void findExpiredJobIds는_Redis_예외_시_빈_집합을_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        assertThat(registry.findExpiredJobIds()).isEmpty();
    }

    @Test
    void hasLiveHeartbeat는_Redis_예외_시_true를_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.score(KEY, "5"))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        assertThat(registry.hasLiveHeartbeat(5L)).isTrue();
    }

    @Test
    void remove는_Redis_예외를_삼킨다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.remove(KEY, "7"))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        assertThatCode(() -> registry.remove(7L)).doesNotThrowAnyException();
    }

    @Test
    void stopHeartbeat은_remove가_Redis_예외를_던져도_전파하지_않는다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.add(anyString(), anyString(), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));
        when(zSetOperations.remove(KEY, "3"))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        ScheduledFuture<?> future = registry.startHeartbeat(3L);

        assertThatCode(() -> registry.stopHeartbeat(3L, future)).doesNotThrowAnyException();
        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    void 정상_상황에서_touch는_now에_timeout을_더한_score로_기록한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        long before = Instant.now().getEpochSecond();

        ScheduledFuture<?> future = registry.startHeartbeat(9L);
        future.cancel(false);

        ArgumentCaptor<Double> score = ArgumentCaptor.forClass(Double.class);
        verify(zSetOperations, atLeast(1)).add(eq(KEY), eq("9"), score.capture());
        assertThat(score.getValue()).isBetween((double) (before + 10), (double) (Instant.now().getEpochSecond() + 10));
    }

    @Test
    void 정상_상황에서_findExpiredJobIds는_조회된_ID를_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenReturn(Set.of("11", "12"));

        assertThat(registry.findExpiredJobIds()).containsExactlyInAnyOrder(11L, 12L);
    }

    @Test
    void 정상_상황에서_hasLiveHeartbeat는_score_만료_여부로_판정한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.score(KEY, "13")).thenReturn((double) (Instant.now().getEpochSecond() + 30));
        when(zSetOperations.score(KEY, "14")).thenReturn((double) (Instant.now().getEpochSecond() - 30));
        when(zSetOperations.score(KEY, "15")).thenReturn(null);

        assertThat(registry.hasLiveHeartbeat(13L)).isTrue();
        assertThat(registry.hasLiveHeartbeat(14L)).isFalse();
        assertThat(registry.hasLiveHeartbeat(15L)).isFalse();
    }

    @Test
    void 정상_상황에서_remove는_ZSET_멤버를_제거한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.remove(KEY, "16")).thenReturn(1L);

        registry.remove(16L);

        verify(zSetOperations).remove(KEY, "16");
    }

    @Test
    void Redis_복구_직후_유예_구간에는_findExpiredJobIds가_빈_집합을_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"))
                .thenReturn(Set.of("21", "22"));

        // 장애로 실패 시각이 기록된다
        assertThat(registry.findExpiredJobIds()).isEmpty();

        // Redis는 복구됐지만 유예(timeout 10초)가 끝나지 않았다
        clock.advance(Duration.ofSeconds(9));
        assertThat(registry.findExpiredJobIds()).isEmpty();
        verify(zSetOperations, times(1)).rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble());

        // 유예가 끝나면 다시 조회한다
        clock.advance(Duration.ofSeconds(2));
        assertThat(registry.findExpiredJobIds()).containsExactlyInAnyOrder(21L, 22L);
    }

    @Test
    void 유예_구간에는_hasLiveHeartbeat가_true를_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.score(KEY, "31"))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        assertThat(registry.hasLiveHeartbeat(31L)).isTrue();

        // 유예 구간에서는 조회 없이 살아있다고 본다
        clock.advance(Duration.ofSeconds(9));
        assertThat(registry.hasLiveHeartbeat(32L)).isTrue();
        verify(zSetOperations, never()).score(KEY, "32");

        // 유예가 끝나면 score를 실제로 조회한다 (미등록 = null → 만료)
        clock.advance(Duration.ofSeconds(2));
        assertThat(registry.hasLiveHeartbeat(32L)).isFalse();
        verify(zSetOperations).score(KEY, "32");
    }

    @Test
    void 유예_구간에도_touch는_계속_시도한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        registry.findExpiredJobIds();
        clock.advance(Duration.ofSeconds(1));

        ScheduledFuture<?> future = registry.startHeartbeat(41L);
        future.cancel(false);

        verify(zSetOperations, atLeastOnce()).add(eq(KEY), eq("41"), anyDouble());
    }

    @Test
    void 유예_구간에도_remove는_계속_시도한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        registry.findExpiredJobIds();
        clock.advance(Duration.ofSeconds(1));

        registry.remove(42L);

        verify(zSetOperations).remove(KEY, "42");
    }

    @Test
    void touch_실패가_findExpiredJobIds_회수를_보류시킨다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.add(anyString(), anyString(), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        ScheduledFuture<?> future = registry.startHeartbeat(43L);
        future.cancel(false);

        clock.advance(Duration.ofSeconds(9));
        assertThat(registry.findExpiredJobIds()).isEmpty();
        verify(zSetOperations, never()).rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble());
    }

    @Test
    void 실패_이력이_없으면_유예가_걸리지_않는다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenReturn(Set.of("51"));
        when(zSetOperations.score(KEY, "52")).thenReturn((double) (clock.instant().getEpochSecond() + 30));

        assertThat(registry.findExpiredJobIds()).containsExactly(51L);
        assertThat(registry.hasLiveHeartbeat(52L)).isTrue();

        verify(zSetOperations).rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble());
        verify(zSetOperations).score(KEY, "52");
    }

    @Test
    void 임계치_전에는_억제_경보를_내지_않는다() {
        givenRedisFailingOnRemove();

        beginOutage();
        continueOutage(Duration.ofSeconds(55));

        assertThat(errorLogs()).isEmpty();
    }

    @Test
    void 억제가_임계치를_넘기면_ERROR로_경보한다() {
        givenRedisFailingOnRemove();

        beginOutage();
        continueOutage(Duration.ofSeconds(60));

        assertThat(errorLogs()).hasSize(1);
        // 지속 시간과 회수 억제 사실이 로그에서 드러나야 "회수할 게 없음"과 구분된다
        assertThat(errorLogs().get(0).getFormattedMessage())
                .contains("60초")
                .contains("회수가 그동안 계속 억제");
    }

    @Test
    void 억제_경보는_임계치_주기로만_재발행된다() {
        givenRedisFailingOnRemove();

        beginOutage();
        continueOutage(Duration.ofSeconds(60));
        assertThat(errorLogs()).hasSize(1);

        // 임계치 주기가 다시 차기 전에는 판정이 여러 번 일어나도 늘어나지 않는다
        continueOutage(Duration.ofSeconds(55));
        assertThat(errorLogs()).hasSize(1);

        continueOutage(Duration.ofSeconds(5));
        assertThat(errorLogs()).hasSize(2);
    }

    @Test
    void 유예가_풀리면_억제_상태가_리셋되고_회복을_남긴다() {
        givenRedisFailingOnRemove();
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenReturn(Set.of());

        beginOutage();
        continueOutage(Duration.ofSeconds(60));
        assertThat(errorLogs()).hasSize(1);

        // 더 이상 실패가 없으면 유예(timeout 10초)가 풀리고 회수가 재개된다
        clock.advance(Duration.ofSeconds(11));
        assertThat(registry.findExpiredJobIds()).isEmpty();
        assertThat(infoLogs()).hasSize(1);
        assertThat(infoLogs().get(0).getFormattedMessage()).contains("회수를 재개");

        // 새 장애는 지속 시간이 0부터 다시 세어진다. 리셋이 안 되면 첫 판정에서 곧바로 경보가 나간다.
        beginOutage();
        continueOutage(Duration.ofSeconds(55));
        assertThat(errorLogs()).hasSize(1);

        continueOutage(Duration.ofSeconds(5));
        assertThat(errorLogs()).hasSize(2);
    }

    private void givenRedisFailingOnRemove() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.remove(eq(KEY), anyString()))
                .thenThrow(new RedisConnectionFailureException("redis down"));
    }

    /** Redis 실패를 한 번 일으켜 억제를 시작시킨다. remove는 유예 중에도 계속 호출되는 경로다. */
    private void beginOutage() {
        registry.remove(99L);
    }

    /**
     * 장애가 이어지는 상황을 재현한다.
     * 실제 운영에서는 touch가 refreshInterval마다 실패해 유예를 재무장하므로,
     * 유예(timeout 10초)보다 짧은 5초 간격으로 실패와 판정을 반복한다.
     */
    private void continueOutage(Duration duration) {
        for (long elapsed = 0; elapsed < duration.getSeconds(); elapsed += 5) {
            clock.advance(Duration.ofSeconds(5));
            registry.remove(99L);
            registry.findExpiredJobIds();
        }
    }

    private List<ILoggingEvent> errorLogs() {
        return logsAt(Level.ERROR);
    }

    private List<ILoggingEvent> infoLogs() {
        return logsAt(Level.INFO);
    }

    private List<ILoggingEvent> logsAt(Level level) {
        return logAppender.list.stream().filter(event -> event.getLevel() == level).toList();
    }

    /** 유예 만료를 실제 대기 없이 검증하기 위한 수동 진행 시계다. */
    static final class MutableClock extends Clock {

        private volatile Instant instant;

        MutableClock(Instant instant) {
            this.instant = instant;
        }

        void advance(Duration duration) {
            this.instant = this.instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
