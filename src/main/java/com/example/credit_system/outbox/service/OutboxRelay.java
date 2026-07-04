package com.example.credit_system.outbox.service;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.outbox.domain.OutboxEntry;
import com.example.credit_system.outbox.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.scheduling", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AppProperties appProperties;

    @Transactional
    @Scheduled(fixedDelayString = "${app.scheduling.outbox-relay-interval-millis:1000}")
    public void relay() {
        List<OutboxEntry> pending = outboxRepository.findBySentFalseOrderByIdAsc();
        for (OutboxEntry entry : pending) {
            kafkaTemplate.send(appProperties.kafka().topic(), entry.getJobId().toString(), entry.getPayload());
            outboxRepository.markSent(entry.getId());
            log.info("outbox 발행: outboxId={}, jobId={}", entry.getId(), entry.getJobId());
        }
    }
}
