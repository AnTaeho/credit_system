package com.example.credit_system.global.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerExecutorConfig {

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
