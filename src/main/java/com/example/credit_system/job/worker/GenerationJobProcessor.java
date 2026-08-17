package com.example.credit_system.job.worker;

import com.example.credit_system.global.scheduler.HeartbeatRegistry;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.service.ConfirmService;
import com.example.credit_system.job.service.FailureService;
import com.example.credit_system.job.stub.GenerationStubClient;
import com.example.credit_system.job.stub.StubGenerationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ScheduledFuture;

/**
 * 이미 선점된 생성 작업의 외부 호출과 결과 반영을 담당한다.
 *
 * 큐 폴링과 선점은 {@link GenerationWorker}에 두고, 오래 걸리는 외부 호출의 lifecycle만 이 클래스에서 관리한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GenerationJobProcessor {

    private final HeartbeatRegistry heartbeatRegistry;
    private final GenerationStubClient stubClient;
    private final ConfirmService confirmService;
    private final FailureService failureService;

    /**
     * 처리 시작부터 종료까지 heartbeat를 유지한다.
     *
     * 어떤 예외가 발생해도 finally에서 heartbeat를 제거해야, 크래시가 아닌 정상 실패 작업이
     * 불필요하게 정체 작업으로 오인되지 않는다.
     */
    public void process(Job job) {
        ScheduledFuture<?> heartbeatFuture = heartbeatRegistry.startHeartbeat(job.getId());
        try {
            String resultUrl = stubClient.generate(job.getPrompt());
            confirmService.confirm(job.getId(), job.getAttemptNo(), resultUrl);
        } catch (StubGenerationException e) {
            // 외부 생성 실패는 정상적인 도메인 결과다. 스케줄러가 재시도 또는 환불을 결정한다.
            failureService.markFailed(job.getId(), job.getAttemptNo());
        } catch (RuntimeException e) {
            // 예기치 못한 오류도 PROCESSING에 방치하지 않고 동일한 복구 경로로 보낸다.
            log.warn("예상하지 못한 생성 작업 실패: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo(), e);
            failureService.markFailed(job.getId(), job.getAttemptNo());
        } finally {
            heartbeatRegistry.stopHeartbeat(job.getId(), heartbeatFuture);
        }
    }
}
