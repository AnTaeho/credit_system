package com.example.credit_system.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        Generation generation,
        Stub stub,
        Heartbeat heartbeat,
        Processing processing
) {

    public record Generation(long cost, int maxAttempts) {
    }

    public record Stub(double failureRate, long minDelayMillis, long maxDelayMillis) {
    }

    /**
     * heartbeat 유효 기간, 갱신 주기, 회수 억제 경보 임계치를 정의한다.
     *
     * @param suppressionAlertSeconds Redis 장애로 회수가 이 시간(초) 넘게 억제되면 ERROR로 경보한다
     */
    public record Heartbeat(long timeoutSeconds, long refreshIntervalSeconds, long suppressionAlertSeconds) {

        /**
         * 복구 유예의 전제를 시작 시점에 검증한다.
         * 유예 길이가 timeoutSeconds인데 갱신 주기가 그보다 길면 살아있는 워커가 유예 안에서 단 한 번도
         * touch에 성공하지 못해, 유예가 풀리는 순간 정상 job이 만료로 회수된다.
         */
        public Heartbeat {
            if (timeoutSeconds < 1) {
                throw new IllegalArgumentException(
                        "heartbeat timeout-seconds는 1 이상이어야 합니다: " + timeoutSeconds);
            }
            if (refreshIntervalSeconds < 1) {
                throw new IllegalArgumentException(
                        "heartbeat refresh-interval-seconds는 1 이상이어야 합니다: " + refreshIntervalSeconds);
            }
            if (refreshIntervalSeconds >= timeoutSeconds) {
                throw new IllegalArgumentException(
                        "heartbeat refresh-interval-seconds는 timeout-seconds보다 작아야 합니다. "
                                + "그렇지 않으면 복구 유예 동안 살아있는 워커가 heartbeat를 갱신하지 못해 정상 job이 회수됩니다: "
                                + "refresh-interval-seconds=" + refreshIntervalSeconds
                                + ", timeout-seconds=" + timeoutSeconds);
            }
            if (suppressionAlertSeconds < 1) {
                throw new IllegalArgumentException(
                        "heartbeat suppression-alert-seconds는 1 이상이어야 합니다: " + suppressionAlertSeconds);
            }
        }
    }

    public record Processing(long timeoutSeconds) {
    }
}
