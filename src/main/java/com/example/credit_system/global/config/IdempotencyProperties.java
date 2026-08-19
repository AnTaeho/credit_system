package com.example.credit_system.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.idempotency")
public record IdempotencyProperties(long retentionDays) {

    public IdempotencyProperties {
        if (retentionDays < 1) {
            throw new IllegalArgumentException("idempotency retention-days는 1 이상이어야 합니다.");
        }
    }
}
