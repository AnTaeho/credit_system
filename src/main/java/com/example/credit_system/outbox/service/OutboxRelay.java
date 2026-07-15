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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.scheduling", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AppProperties appProperties;

    /** 미발송 outbox를 Kafka에 전달하고 성공 건을 표시한다. */
    @Scheduled(fixedDelayString = "${app.scheduling.outbox-relay-interval-millis:1000}")
    public void relay() {
        List<OutboxEntry> pending = outboxRepository.findBySentFalseOrderByIdAsc();
        for (OutboxEntry entry : pending) {
            try {
                kafkaTemplate.send(appProperties.kafka().topic(), entry.getJobId().toString(), entry.getPayload())
                        .get(10, TimeUnit.SECONDS);
                outboxRepository.markSent(entry.getId());
                log.info("outbox 발행: outboxId={}, jobId={}", entry.getId(), entry.getJobId());
            } catch (ExecutionException | TimeoutException e) {
                log.warn("outbox 발행 실패, 다음 주기에 재시도: outboxId={}, jobId={}",
                        entry.getId(), entry.getJobId(), e);
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
