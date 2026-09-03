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

    public record Heartbeat(long timeoutSeconds, long refreshIntervalSeconds) {

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
                                + "그렇지 않으면 갱신 주기가 돌아오기 전에 heartbeat가 만료되어 살아있는 job이 회수됩니다: "
                                + "refresh-interval-seconds=" + refreshIntervalSeconds
                                + ", timeout-seconds=" + timeoutSeconds);
            }
        }
    }

    public record Processing(long timeoutSeconds) {
    }
}
