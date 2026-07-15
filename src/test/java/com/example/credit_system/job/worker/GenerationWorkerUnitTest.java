package com.example.credit_system.job.worker;

import com.example.credit_system.global.scheduler.HeartbeatRegistry;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.ConfirmService;
import com.example.credit_system.job.service.FailureService;
import com.example.credit_system.job.stub.GenerationStubClient;
import com.example.credit_system.job.stub.StubGenerationException;
import com.example.credit_system.outbox.domain.GenerationJobMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.concurrent.ScheduledFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GenerationWorkerUnitTest {

    @Mock JobRepository jobRepository;
    @Mock HeartbeatRegistry heartbeatRegistry;
    @Mock GenerationStubClient stubClient;
    @Mock ConfirmService confirmService;
    @Mock FailureService failureService;
    @Mock ObjectMapper objectMapper;
    @Mock ScheduledFuture<?> heartbeatFuture;

    GenerationWorker worker;
    GenerationJobMessage message;

    @BeforeEach
    void setUp() {
        worker = new GenerationWorker(jobRepository, heartbeatRegistry, stubClient,
                confirmService, failureService, objectMapper);
        message = new GenerationJobMessage(1L, 10L, 2, "cat");
        when(objectMapper.readValue("payload", GenerationJobMessage.class)).thenReturn(message);
    }

    @Test
    void 종결_상태나_낡은_attempt의_메시지는_외부_작업을_시작하지_않는다() {
        when(jobRepository.startProcessingIfAttemptMatches(eq(1L), eq(2), any(Instant.class)))
                .thenReturn(0);

        worker.consume("payload");

        verify(heartbeatRegistry, never()).startHeartbeat(any());
        verify(stubClient, never()).generate(any());
        verify(confirmService, never()).confirm(any(), eq(2), any());
        verify(failureService, never()).markFailed(any(), eq(2));
    }

    @Test
    void 생성_실패는_FAILED로_기록하고_heartbeat를_항상_정리한다() {
        when(jobRepository.startProcessingIfAttemptMatches(eq(1L), eq(2), any(Instant.class)))
                .thenReturn(1);
        doReturn(heartbeatFuture).when(heartbeatRegistry).startHeartbeat(1L);
        when(stubClient.generate("cat")).thenThrow(new StubGenerationException("cat"));

        worker.consume("payload");

        verify(failureService).markFailed(1L, 2);
        verify(confirmService, never()).confirm(any(), eq(2), any());
        verify(heartbeatRegistry).stopHeartbeat(1L, heartbeatFuture);
    }

    @Test
    void 예상하지_못한_예외는_상위로_전파하되_heartbeat는_정리한다() {
        when(jobRepository.startProcessingIfAttemptMatches(eq(1L), eq(2), any(Instant.class)))
                .thenReturn(1);
        doReturn(heartbeatFuture).when(heartbeatRegistry).startHeartbeat(1L);
        when(stubClient.generate("cat")).thenThrow(new IllegalStateException("unexpected"));

        assertThatThrownBy(() -> worker.consume("payload"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("unexpected");

        verify(failureService, never()).markFailed(any(), eq(2));
        verify(heartbeatRegistry).stopHeartbeat(1L, heartbeatFuture);
    }
}
