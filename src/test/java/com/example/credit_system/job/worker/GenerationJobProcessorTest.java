package com.example.credit_system.job.worker;

import com.example.credit_system.scheduler.HeartbeatRegistry;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.service.JobLifecycleService;
import com.example.credit_system.job.stub.GenerationStubClient;
import com.example.credit_system.global.exception.StubGenerationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.ScheduledFuture;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GenerationJobProcessorTest {

    @Mock HeartbeatRegistry heartbeatRegistry;
    @Mock GenerationStubClient stubClient;
    @Mock JobLifecycleService jobLifecycleService;
    @Mock ScheduledFuture<?> heartbeatFuture;

    GenerationJobProcessor processor;
    Job job;

    @BeforeEach
    void setUp() {
        processor = new GenerationJobProcessor(heartbeatRegistry, stubClient, jobLifecycleService);
        job = Job.hold(10L, 100L, "cat");
        ReflectionTestUtils.setField(job, "id", 1L);
        doReturn(heartbeatFuture).when(heartbeatRegistry).startHeartbeat(1L);
    }

    @Test
    void 성공하면_confirm하고_heartbeat를_정리한다() {
        when(stubClient.generate("cat")).thenReturn("https://example.test/cat.png");

        processor.process(job);

        verify(jobLifecycleService).confirm(1L, 0, "https://example.test/cat.png");
        verify(heartbeatRegistry).stopHeartbeat(1L, heartbeatFuture);
    }

    @Test
    void 생성_실패는_FAILED로_기록하고_heartbeat를_정리한다() {
        when(stubClient.generate("cat")).thenThrow(new StubGenerationException("cat"));

        processor.process(job);

        verify(jobLifecycleService).markFailed(1L, 0);
        verify(jobLifecycleService, never()).confirm(1L, 0, "https://example.test/cat.png");
        verify(heartbeatRegistry).stopHeartbeat(1L, heartbeatFuture);
    }

    @Test
    void 결과_반영이_실패해도_재시도가_성공하면_결과를_살린다() {
        when(stubClient.generate("cat")).thenReturn("https://example.test/cat.png");
        doThrow(new IllegalStateException("database unavailable"))
                .doNothing()
                .when(jobLifecycleService).confirm(1L, 0, "https://example.test/cat.png");

        processor.process(job);

        verify(jobLifecycleService, times(2)).confirm(1L, 0, "https://example.test/cat.png");
        verify(jobLifecycleService, never()).markFailed(1L, 0);
        verify(heartbeatRegistry).stopHeartbeat(1L, heartbeatFuture);
    }

    @Test
    void 결과_반영_재시도를_모두_소진하면_FAILED로_바꾸지_않고_PROCESSING을_유지한다() {
        when(stubClient.generate("cat")).thenReturn("https://example.test/cat.png");
        doThrow(new IllegalStateException("database unavailable"))
                .when(jobLifecycleService).confirm(1L, 0, "https://example.test/cat.png");

        processor.process(job);

        verify(jobLifecycleService, times(3)).confirm(1L, 0, "https://example.test/cat.png");
        verify(jobLifecycleService, never()).markFailed(1L, 0);
        verify(heartbeatRegistry).stopHeartbeat(1L, heartbeatFuture);
    }
}
