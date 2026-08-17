package com.example.credit_system.job.worker;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** DB 작업 큐를 소비하는 워커의 실행 상한을 정의한다. */
@ConfigurationProperties(prefix = "app.worker")
public record WorkerProperties(
        boolean enabled,
        int batchSize,
        int concurrency
) {

    /** 잘못된 상한으로 작업을 영구 대기시키지 않도록 애플리케이션 시작 시 검증한다. */
    public WorkerProperties {
        if (batchSize < 1 || concurrency < 1) {
            throw new IllegalArgumentException("worker batch-size와 concurrency는 1 이상이어야 합니다.");
        }
    }
}
