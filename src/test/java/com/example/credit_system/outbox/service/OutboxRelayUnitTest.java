package com.example.credit_system.outbox.service;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.outbox.domain.OutboxEntry;
import com.example.credit_system.outbox.repository.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayUnitTest {

    @Mock
    OutboxRepository outboxRepository;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    OutboxRelay outboxRelay;

    @BeforeEach
    void setUp() {
        AppProperties appProperties = new AppProperties(
                null, null, null, new AppProperties.Kafka("generation-jobs", 1), null, null);
        outboxRelay = new OutboxRelay(outboxRepository, kafkaTemplate, appProperties);
    }

    private static OutboxEntry entry(long id, long jobId) {
        OutboxEntry entry = new OutboxEntry(jobId, "{\"jobId\":" + jobId + "}");
        ReflectionTestUtils.setField(entry, "id", id);
        return entry;
    }

    @Test
    void 발행이_실패하면_markSent를_호출하지_않는다() {
        OutboxEntry pending = entry(1L, 10L);
        when(outboxRepository.findBySentFalseOrderByIdAsc()).thenReturn(List.of(pending));
        when(kafkaTemplate.send(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        outboxRelay.relay();

        verify(outboxRepository, never()).markSent(anyLong());
    }

    @Test
    void 첫_entry_발행이_실패하면_다음_entry는_시도하지_않는다() {
        OutboxEntry first = entry(1L, 10L);
        OutboxEntry second = entry(2L, 20L);
        when(outboxRepository.findBySentFalseOrderByIdAsc()).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        outboxRelay.relay();

        verify(kafkaTemplate, org.mockito.Mockito.times(1)).send(any(), any(), any());
        verify(outboxRepository, never()).markSent(anyLong());
    }

    @Test
    void 발행이_성공하면_markSent를_호출한다() {
        OutboxEntry pending = entry(1L, 10L);
        when(outboxRepository.findBySentFalseOrderByIdAsc()).thenReturn(List.of(pending));
        when(kafkaTemplate.send(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        outboxRelay.relay();

        verify(outboxRepository).markSent(1L);
    }

    @Test
    void 발행_대기_중_인터럽트되면_상태를_보존하고_다음_항목을_처리하지_않는다() {
        OutboxEntry first = entry(1L, 10L);
        OutboxEntry second = entry(2L, 20L);
        when(outboxRepository.findBySentFalseOrderByIdAsc()).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(new CompletableFuture<>());

        Thread.currentThread().interrupt();
        try {
            outboxRelay.relay();

            verify(kafkaTemplate, org.mockito.Mockito.times(1)).send(any(), any(), any());
            verify(outboxRepository, never()).markSent(anyLong());
            org.assertj.core.api.Assertions.assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
