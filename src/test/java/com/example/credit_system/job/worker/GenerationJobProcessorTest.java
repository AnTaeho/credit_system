package com.example.credit_system.job.worker;

import com.example.credit_system.global.scheduler.HeartbeatRegistry;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.service.ConfirmService;
import com.example.credit_system.job.service.FailureService;
import com.example.credit_system.job.stub.GenerationStubClient;
import com.example.credit_system.job.stub.StubGenerationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.ScheduledFuture;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GenerationJobProcessorTest {

    @Mock HeartbeatRegistry heartbeatRegistry;
    @Mock GenerationStubClient stubClient;
    @Mock ConfirmService confirmService;
    @Mock FailureService failureService;
    @Mock ScheduledFuture<?> heartbeatFuture;

    GenerationJobProcessor processor;
    Job job;

    @BeforeEach
    void setUp() {
        processor = new GenerationJobProcessor(heartbeatRegistry, stubClient, confirmService, failureService);
        job = Job.hold(10L, 100L, "cat");
        ReflectionTestUtils.setField(job, "id", 1L);
        doReturn(heartbeatFuture).when(heartbeatRegistry).startHeartbeat(1L);
    }

    @Test
    void 성공하면_confirm하고_heartbeat를_정리한다() {
        when(stubClient.generate("cat")).thenReturn("https://example.test/cat.png");

        processor.process(job);

        verify(confirmService).confirm(1L, 0, "https://example.test/cat.png");
        verify(heartbeatRegistry).stopHeartbeat(1L, heartbeatFuture);
    }

    @Test
    void 생성_실패는_FAILED로_기록하고_heartbeat를_정리한다() {
        when(stubClient.generate("cat")).thenThrow(new StubGenerationException("cat"));

        processor.process(job);

        verify(failureService).markFailed(1L, 0);
        verify(confirmService, never()).confirm(1L, 0, "https://example.test/cat.png");
        verify(heartbeatRegistry).stopHeartbeat(1L, heartbeatFuture);
    }
}
