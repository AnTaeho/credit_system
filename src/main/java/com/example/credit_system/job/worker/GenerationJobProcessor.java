package com.example.credit_system.job.worker;

import com.example.credit_system.scheduler.HeartbeatRegistry;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.service.ConfirmService;
import com.example.credit_system.job.service.FailureService;
import com.example.credit_system.job.stub.GenerationStubClient;
import com.example.credit_system.global.exception.StubGenerationException;
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

    /**
     * confirm 재시도 상한과 간격.
     *
     * 순간적인 DB 장애를 넘기는 것이 목적이라 짧게 잡는다. 길게 끌면 heartbeat를 붙든 채 executor 슬롯을
     * 오래 점유해 다른 작업의 처리량을 떨어뜨린다.
     */
    private static final int CONFIRM_MAX_ATTEMPTS = 3;
    private static final long CONFIRM_RETRY_DELAY_MILLIS = 200L;

    private final HeartbeatRegistry heartbeatRegistry;
    private final GenerationStubClient stubClient;
    private final ConfirmService confirmService;
    private final FailureService failureService;

    /**
     * 처리 시작부터 종료까지 heartbeat를 유지한다.
     *
     * 어떤 예외가 발생해도 finally에서 heartbeat를 제거해야, 크래시가 아닌 정상 실패 작업이
     * 불필요하게 정체 작업으로 오인되지 않는다. 재시도 중에도 heartbeat는 살아 있어야 하므로
     * 제거는 가장 바깥 finally에서만 한다.
     */
    public void process(Job job) {
        ScheduledFuture<?> heartbeatFuture = heartbeatRegistry.startHeartbeat(job.getId());
        try {
            String resultUrl;
            try {
                resultUrl = stubClient.generate(job.getPrompt());
            } catch (StubGenerationException e) {
                // 외부 생성 실패는 정상적인 도메인 결과다. 스케줄러가 재시도 또는 환불을 결정한다.
                failureService.markFailed(job.getId(), job.getAttemptNo());
                return;
            }

            try {
                confirmWithRetry(job, resultUrl);
            } catch (RuntimeException e) {
                // 재시도를 모두 소진했다. FAILED로 바꾸지 않는다고 해서 유료 외부 생성의 중복 실행이 막히는 것은 아니다.
                // heartbeat가 곧 끊기므로 DeadJobSchedulerTask가 processing timeout 뒤에 이 작업을 회수해
                // 재시도시키고, 그때 외부 생성이 다시 실행된다. 여기서 얻는 것은 중복 실행 차단이 아니라
                // timeout만큼의 지연과, 회수 경로 한 곳으로 다음 시도를 몰아주는 통제권이다.
                // 이미 만들어진 resultUrl은 이 시점에 유실된다.
                log.error("생성 결과 반영 재시도 소진, timeout 회수 대기: jobId={}, attemptNo={}",
                        job.getId(), job.getAttemptNo(), e);
            }
        } finally {
            heartbeatRegistry.stopHeartbeat(job.getId(), heartbeatFuture);
        }
    }

    /**
     * 결과 반영을 짧게 재시도해, 순간적인 DB 장애 때문에 이미 만들어진 결과를 버리지 않도록 한다.
     *
     * 재시도가 안전한 이유는 {@code JobRepository.completeIfAttemptMatches}가 status=PROCESSING과
     * attemptNo 일치를 조건으로 걸기 때문이다. 앞선 시도가 실제로 반영됐다면 다음 시도는 0건 갱신으로
     * 흘려보내지고, 회수 경로가 이미 작업을 되돌렸다면 뒤늦은 confirm이 그 결정을 덮어쓰지 못한다.
     *
     * @throws RuntimeException 마지막 시도까지 실패했을 때의 원인 예외
     */
    private void confirmWithRetry(Job job, String resultUrl) {
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= CONFIRM_MAX_ATTEMPTS; attempt++) {
            try {
                confirmService.confirm(job.getId(), job.getAttemptNo(), resultUrl);
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
                    // shutdown 신호다. 인터럽트 플래그를 복원해 상위 executor가 종료를 이어가게 하고 즉시 포기한다.
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        throw lastFailure;
    }
}
