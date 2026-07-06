package com.example.credit_system.job.worker;

import com.example.credit_system.global.scheduler.HeartbeatRegistry;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.ConfirmService;
import com.example.credit_system.job.service.FailureService;
import com.example.credit_system.job.stub.GenerationStubClient;
import com.example.credit_system.job.stub.StubGenerationException;
import com.example.credit_system.outbox.domain.GenerationJobMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class GenerationWorker {

    private final JobRepository jobRepository;
    private final HeartbeatRegistry heartbeatRegistry;
    private final GenerationStubClient stubClient;
    private final ConfirmService confirmService;
    private final FailureService failureService;
    private final ObjectMapper objectMapper;

    // 파티션 수만큼 컨슈머 스레드를 띄워 처리량을 파티션 수에 맞춘다.
    @KafkaListener(topics = "${app.kafka.topic}", concurrency = "${app.kafka.partitions}")
    public void consume(String payload) {
        GenerationJobMessage message = objectMapper.readValue(payload, GenerationJobMessage.class);

        int updated = jobRepository.updateStatusIfAttemptMatches(
                message.jobId(), JobStatus.PROCESSING, message.attemptNo(), Instant.now());
        if (updated == 0) {
            log.info("무효한 메시지 무시: jobId={}, attemptNo={}", message.jobId(), message.attemptNo());
            return;
        }

        ScheduledFuture<?> heartbeatFuture = heartbeatRegistry.startHeartbeat(message.jobId());
        try {
            String resultUrl = stubClient.generate(message.prompt());
            confirmService.confirm(message.jobId(), message.attemptNo(), resultUrl);
        } catch (StubGenerationException e) {
            failureService.markFailed(message.jobId(), message.attemptNo());
        } finally {
            heartbeatRegistry.stopHeartbeat(message.jobId(), heartbeatFuture);
        }
    }
}
