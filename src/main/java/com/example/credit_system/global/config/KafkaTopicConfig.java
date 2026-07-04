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

    @Bean
    public NewTopic generationJobsTopic() {
        return TopicBuilder.name(appProperties.kafka().topic())
                .partitions(3)
                .replicas(1)
                .build();
    }
}
