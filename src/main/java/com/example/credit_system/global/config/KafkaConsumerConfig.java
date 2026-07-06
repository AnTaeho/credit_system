package com.example.credit_system.global.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerConfig {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // 1회 최초 시도 + 2회 재시도(1초 간격) = 총 3회 시도 후 DLT(<topic>.DLT)로 발행한다.
    // 파싱 실패(poison message) 등 재시도로 해결되지 않는 예외도 이 한도 안에서 DLT로 격리되어
    // 파티션이 막히지 않는다.
    // DeadLetterPublishingRecoverer의 기본 목적지 리졸버는 "<topic>-dlt"(하이픈) 규칙을 쓰므로,
    // KafkaTopicConfig에서 만든 "<topic>.DLT" 토픽으로 보내려면 리졸버를 명시해야 한다.
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }
}
