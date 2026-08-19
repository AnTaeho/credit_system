package com.example.credit_system.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.worker")
public record WorkerProperties(
        boolean enabled,
        int batchSize,
        int concurrency
) {

    public WorkerProperties {
        if (batchSize < 1 || concurrency < 1) {
            throw new IllegalArgumentException("worker batch-size와 concurrency는 1 이상이어야 합니다.");
        }
    }
}
