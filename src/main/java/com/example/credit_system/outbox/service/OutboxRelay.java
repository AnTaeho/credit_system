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

    // 블로킹 Kafka I/O를 DB 트랜잭션(커넥션 점유) 안에 두지 않기 위해 relay() 자체는 트랜잭션이 없다.
    // markSent는 리포지토리 메서드 자체에 선언된 트랜잭션으로 커밋된다.
    @Scheduled(fixedDelayString = "${app.scheduling.outbox-relay-interval-millis:1000}")
    public void relay() {
        List<OutboxEntry> pending = outboxRepository.findBySentFalseOrderByIdAsc();
        for (OutboxEntry entry : pending) {
            try {
                // 브로커 ack를 받은 뒤에만 markSent 한다. ack 전에 markSent 하면 전송이 실제로
                // 실패했을 때 outbox가 sent=true로 남아 메시지가 영구 유실된다. 반대로 ack 후
                // markSent가 지연/실패해 재전송되더라도 컨슈머 쪽 attemptNo fencing이 중복을
                // 흡수하므로, "최소 1회 전송" 쪽을 택한다.
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
