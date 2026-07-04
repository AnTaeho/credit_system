package com.example.credit_system.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        Generation generation,
        Stub stub,
        Heartbeat heartbeat,
        Kafka kafka
) {

    public record Generation(long cost, int maxAttempts) {
    }

    public record Stub(double failureRate, long minDelayMillis, long maxDelayMillis) {
    }

    public record Heartbeat(long timeoutSeconds, long refreshIntervalSeconds) {
    }

    public record Kafka(String topic) {
    }
}
