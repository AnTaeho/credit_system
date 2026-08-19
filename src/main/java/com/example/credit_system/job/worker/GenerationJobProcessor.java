package com.example.credit_system.job.worker;

import com.example.credit_system.scheduler.HeartbeatRegistry;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.service.JobLifecycleService;
import com.example.credit_system.job.stub.GenerationStubClient;
import com.example.credit_system.global.exception.StubGenerationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ScheduledFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class GenerationJobProcessor {

    private static final int CONFIRM_MAX_ATTEMPTS = 3;
    private static final long CONFIRM_RETRY_DELAY_MILLIS = 200L;

    private final HeartbeatRegistry heartbeatRegistry;
    private final GenerationStubClient stubClient;
    private final JobLifecycleService jobLifecycleService;

    public void process(Job job) {
        ScheduledFuture<?> heartbeatFuture = heartbeatRegistry.startHeartbeat(job.getId());
        try {
            String resultUrl;
            try {
                resultUrl = stubClient.generate(job.getPrompt());
            } catch (StubGenerationException e) {
                jobLifecycleService.markFailed(job.getId(), job.getAttemptNo());
                return;
            }

            try {
                confirmWithRetry(job, resultUrl);
            } catch (RuntimeException e) {
                log.error("생성 결과 반영 재시도 소진, timeout 회수 대기: jobId={}, attemptNo={}",
                        job.getId(), job.getAttemptNo(), e);
            }
        } finally {
            heartbeatRegistry.stopHeartbeat(job.getId(), heartbeatFuture);
        }
    }

    private void confirmWithRetry(Job job, String resultUrl) {
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= CONFIRM_MAX_ATTEMPTS; attempt++) {
            try {
                jobLifecycleService.confirm(job.getId(), job.getAttemptNo(), resultUrl);
                return;
            } catch (RuntimeException e) {
                lastFailure = e;
                log.warn("생성 결과 반영 실패: jobId={}, attemptNo={}, 시도={}/{}",
                        job.getId(), job.getAttemptNo(), attempt, CONFIRM_MAX_ATTEMPTS, e);
            }
            if (attempt < CONFIRM_MAX_ATTEMPTS) {
                try {
                    Thread.sleep(CONFIRM_RETRY_DELAY_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        throw lastFailure;
    }
}
