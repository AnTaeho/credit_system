package com.example.credit_system.global.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTopicConfig {

    private final AppProperties appProperties;

    /** 생성 작업용 Kafka 토픽을 구성한다. */
    @Bean
    public NewTopic generationJobsTopic() {
        return TopicBuilder.name(appProperties.kafka().topic())
                .partitions(appProperties.kafka().partitions())
                .replicas(1)
                .build();
    }

    /** 원본과 같은 파티션 수의 DLT를 구성한다. */
    @Bean
    public NewTopic generationJobsDeadLetterTopic() {
        return TopicBuilder.name(appProperties.kafka().topic() + ".DLT")
                .partitions(appProperties.kafka().partitions())
                .replicas(1)
                .build();
    }
}
