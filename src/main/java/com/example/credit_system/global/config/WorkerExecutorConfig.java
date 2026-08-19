package com.example.credit_system.global.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/** 폴링 스케줄러와 분리된 외부 생성 작업 전용 executor를 구성한다. */
@Configuration
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerExecutorConfig {

    /**
     * queue 없이 고정된 수의 외부 작업만 실행한다.
     *
     * 대기열은 RDB의 HOLDING 상태가 담당하므로 executor 내부 queue에 작업을 쌓지 않는다.
     */
    @Bean("generationWorkerExecutor")
    public ThreadPoolTaskExecutor generationWorkerExecutor(WorkerProperties workerProperties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(workerProperties.concurrency());
        executor.setMaxPoolSize(workerProperties.concurrency());
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("generation-worker-");
        executor.initialize();
        return executor;
    }
}
