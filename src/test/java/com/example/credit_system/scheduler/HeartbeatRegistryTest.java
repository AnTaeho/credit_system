package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.config.WorkerProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

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

    @BeforeEach
    void setUp() {
        AppProperties appProperties = new AppProperties(
                new AppProperties.Generation(100L, 3),
                null,
                new AppProperties.Heartbeat(10, 1, 60),
                new AppProperties.Processing(60));
        clock = new MutableClock(Instant.now());
        WorkerProperties workerProperties = new WorkerProperties(true, 20, 3);
        registry = new HeartbeatRegistry(redisTemplate, appProperties, workerProperties, clock);
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
    }

    @Test
    void refreshHeartbeat가_Redis_예외를_삼키고_갱신_스레드를_죽이지_않는다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.add(anyString(), anyString(), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        ScheduledFuture<?> future = registry.startHeartbeat(1L, 0);

        verify(zSetOperations, timeout(5000).atLeast(3)).add(eq(KEY), eq("1:0"), anyDouble());
        assertThat(future.isDone()).isFalse();
        future.cancel(false);
    }

    @Test
    void findExpiredAttempts는_Redis_예외_시_빈_집합을_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        assertThat(registry.findExpiredAttempts()).isEmpty();
    }

    @Test
    void hasLiveHeartbeat는_Redis_예외_시_true를_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.score(KEY, "5:0"))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        assertThat(registry.hasLiveHeartbeat(5L, 0)).isTrue();
    }

    @Test
    void removeHeartbeat는_Redis_예외를_삼킨다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.remove(KEY, "7:0"))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        assertThatCode(() -> registry.removeHeartbeat(7L, 0)).doesNotThrowAnyException();
    }

    @Test
    void stopHeartbeat은_removeHeartbeat가_Redis_예외를_던져도_전파하지_않는다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.add(anyString(), anyString(), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));
        when(zSetOperations.remove(KEY, "3:0"))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        ScheduledFuture<?> future = registry.startHeartbeat(3L, 0);

        assertThatCode(() -> registry.stopHeartbeat(3L, 0, future)).doesNotThrowAnyException();
        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    void 정상_상황에서_refreshHeartbeat는_now에_timeout을_더한_score로_기록한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        long before = Instant.now().getEpochSecond();

        ScheduledFuture<?> future = registry.startHeartbeat(9L, 0);
        future.cancel(false);

        ArgumentCaptor<Double> score = ArgumentCaptor.forClass(Double.class);
        verify(zSetOperations, atLeast(1)).add(eq(KEY), eq("9:0"), score.capture());
        assertThat(score.getValue()).isBetween((double) (before + 10), (double) (Instant.now().getEpochSecond() + 10));
    }

    @Test
    void 정상_상황에서_findExpiredAttempts는_조회된_attempt를_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenReturn(Set.of("11:0", "12:3"));

        assertThat(registry.findExpiredAttempts())
                .containsExactlyInAnyOrder(new JobAttempt(11L, 0), new JobAttempt(12L, 3));
    }

    @Test
    void findExpiredAttempts는_깨진_멤버를_zset에서_제거하고_정상_멤버만_돌려준다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenReturn(Set.of("12", "abc", "1:x", "20:5"));

        assertThat(registry.findExpiredAttempts()).containsExactly(new JobAttempt(20L, 5));

        verify(zSetOperations).remove(KEY, "12");
        verify(zSetOperations).remove(KEY, "abc");
        verify(zSetOperations).remove(KEY, "1:x");
        verify(zSetOperations, never()).remove(KEY, "20:5");
    }

    @Test
    void 정상_상황에서_hasLiveHeartbeat는_score_만료_여부로_판정한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.score(KEY, "13:0")).thenReturn((double) (Instant.now().getEpochSecond() + 30));
        when(zSetOperations.score(KEY, "14:0")).thenReturn((double) (Instant.now().getEpochSecond() - 30));
        when(zSetOperations.score(KEY, "15:0")).thenReturn(null);

        assertThat(registry.hasLiveHeartbeat(13L, 0)).isTrue();
        assertThat(registry.hasLiveHeartbeat(14L, 0)).isFalse();
        assertThat(registry.hasLiveHeartbeat(15L, 0)).isFalse();
    }

    @Test
    void 정상_상황에서_removeHeartbeat는_ZSET_멤버를_제거한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.remove(KEY, "16:0")).thenReturn(1L);

        registry.removeHeartbeat(16L, 0);

        verify(zSetOperations).remove(KEY, "16:0");
    }

    @Test
    void stopHeartbeat은_같은_jobId의_다른_attempt_heartbeat를_지우지_않는다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.add(anyString(), anyString(), anyDouble())).thenReturn(true);

        ScheduledFuture<?> future = registry.startHeartbeat(1L, 0);

        registry.stopHeartbeat(1L, 0, future);

        verify(zSetOperations).remove(KEY, "1:0");
        verify(zSetOperations, never()).remove(KEY, "1:1");
    }

    @Test
    void Redis_복구_직후_유예_구간에는_findExpiredAttempts가_빈_집합을_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"))
                .thenReturn(Set.of("21:0", "22:0"));

        assertThat(registry.findExpiredAttempts()).isEmpty();

        clock.advance(Duration.ofSeconds(9));
        assertThat(registry.findExpiredAttempts()).isEmpty();
        verify(zSetOperations, times(1)).rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble());

        clock.advance(Duration.ofSeconds(2));
        assertThat(registry.findExpiredAttempts())
                .containsExactlyInAnyOrder(new JobAttempt(21L, 0), new JobAttempt(22L, 0));
    }

    @Test
    void 유예_구간에는_hasLiveHeartbeat가_true를_반환한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.score(KEY, "31:0"))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        assertThat(registry.hasLiveHeartbeat(31L, 0)).isTrue();

        clock.advance(Duration.ofSeconds(9));
        assertThat(registry.hasLiveHeartbeat(32L, 0)).isTrue();
        verify(zSetOperations, never()).score(KEY, "32:0");

        clock.advance(Duration.ofSeconds(2));
        assertThat(registry.hasLiveHeartbeat(32L, 0)).isFalse();
        verify(zSetOperations).score(KEY, "32:0");
    }

    @Test
    void 유예_구간에도_refreshHeartbeat는_계속_시도한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        registry.findExpiredAttempts();
        clock.advance(Duration.ofSeconds(1));

        ScheduledFuture<?> future = registry.startHeartbeat(41L, 0);
        future.cancel(false);

        verify(zSetOperations, atLeastOnce()).add(eq(KEY), eq("41:0"), anyDouble());
    }

    @Test
    void 유예_구간에도_removeHeartbeat는_계속_시도한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        registry.findExpiredAttempts();
        clock.advance(Duration.ofSeconds(1));

        registry.removeHeartbeat(42L, 0);

        verify(zSetOperations).remove(KEY, "42:0");
    }

    @Test
    void refreshHeartbeat_실패가_findExpiredAttempts_회수를_보류시킨다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.add(anyString(), anyString(), anyDouble()))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        ScheduledFuture<?> future = registry.startHeartbeat(43L, 0);
        future.cancel(false);

        clock.advance(Duration.ofSeconds(9));
        assertThat(registry.findExpiredAttempts()).isEmpty();
        verify(zSetOperations, never()).rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble());
    }

    @Test
    void 실패_이력이_없으면_유예가_걸리지_않는다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble()))
                .thenReturn(Set.of("51:0"));
        when(zSetOperations.score(KEY, "52:0")).thenReturn((double) (clock.instant().getEpochSecond() + 30));

        assertThat(registry.findExpiredAttempts()).containsExactly(new JobAttempt(51L, 0));
        assertThat(registry.hasLiveHeartbeat(52L, 0)).isTrue();

        verify(zSetOperations).rangeByScore(eq(KEY), eq(Double.NEGATIVE_INFINITY), anyDouble());
        verify(zSetOperations).score(KEY, "52:0");
    }

    @Test
    void heartbeat_스레드_풀은_워커_동시_실행_수만큼_만들어진다() {
        ScheduledThreadPoolExecutor executor =
                (ScheduledThreadPoolExecutor) ReflectionTestUtils.getField(registry, "executor");

        assertThat(executor.getCorePoolSize()).isEqualTo(3);
    }
}
