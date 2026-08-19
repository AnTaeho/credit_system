package com.example.credit_system.job.worker;

import com.example.credit_system.global.config.WorkerProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.Semaphore;

/** DB의 HOLDING 작업을 조회하고, 하나의 워커만 실행하도록 선점한다. */
@Slf4j
@Component
@ConditionalOnExpression("${app.scheduling.enabled:true} and ${app.worker.enabled:true}")
public class GenerationWorker {

    private final JobRepository jobRepository;
    private final GenerationJobProcessor jobProcessor;
    private final TaskExecutor workerExecutor;
    private final int batchSize;
    private final Semaphore availableWorkers;

    /** 작업 큐 의존성, bounded executor, 한 주기당 처리 상한을 구성한다. */
    public GenerationWorker(JobRepository jobRepository,
                            GenerationJobProcessor jobProcessor,
                            @Qualifier("generationWorkerExecutor") TaskExecutor workerExecutor,
                            WorkerProperties workerProperties) {
        this.jobRepository = jobRepository;
        this.jobProcessor = jobProcessor;
        this.workerExecutor = workerExecutor;
        this.batchSize = workerProperties.batchSize();
        this.availableWorkers = new Semaphore(workerProperties.concurrency());
    }

    /**
     * DB 큐에서 설정된 개수만큼 대기 작업을 읽어 선점 후보로 넘긴다.
     *
     * 한 주기가 오래 걸리거나 작업 수가 급증해도 DB와 워커 스레드 사용량이 무한히 커지지 않는다.
     * 여러 인스턴스가 같은 목록을 읽어도 괜찮다. 실제 소유권은 아래 조건부 UPDATE가 결정한다.
     */
    @Scheduled(fixedDelayString = "${app.scheduling.worker-interval-millis:500}")
    public void processPendingJobs() {
        for (Job job : jobRepository.findByStatusOrderByIdAsc(JobStatus.HOLDING, PageRequest.of(0, batchSize))) {
            if (!availableWorkers.tryAcquire()) {
                return;
            }
            // permit 획득과 반납을 이 한 곳에 모아둔다. 선점 UPDATE가 DB 장애로 던져도 permit이 새지 않아야
            // 워커가 재시작 없이는 복구 불가능한 영구 정지 상태에 빠지지 않는다.
            boolean handedOff = false;
            try {
                handedOff = claimAndDispatch(job);
            } catch (RuntimeException e) {
                // 한 작업의 실패가 루프를 중단시키면 남은 작업이 통째로 다음 주기로 밀리므로 여기서 끊는다.
                log.warn("생성 작업 선점 실패: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo(), e);
            } finally {
                // dispatch에 성공한 경우에만 반납 책임이 executor 스레드(processAndRelease)로 넘어간다.
                // 그 경로에서 여기서도 반납하면 이중 반납이 되어 동시 실행 상한이 무너진다.
                if (!handedOff) {
                    availableWorkers.release();
                }
            }
        }
    }

    /**
     * executor 슬롯을 확보한 작업만 HOLDING에서 PROCESSING으로 선점해 전용 executor로 넘긴다.
     *
     * 슬롯을 먼저 확보하면 executor 대기열에 PROCESSING 작업이 쌓여 heartbeat 없이 만료되는 것을 막는다.
     *
     * @return executor가 작업을 받아들여 permit 반납 책임이 {@code processAndRelease}로 넘어갔으면 true.
     *         false면 호출자가 permit을 반납해야 한다.
     */
    boolean claimAndDispatch(Job job) {
        int updated = jobRepository.startProcessingIfAttemptMatches(
                job.getId(), job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("다른 워커가 선점했거나 무효한 작업 무시: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return false;
        }
        try {
            workerExecutor.execute(() -> processAndRelease(job));
            return true;
        } catch (RuntimeException e) {
            log.warn("생성 작업 executor 위임 실패: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo(), e);
            rollbackToHolding(job);
            return false;
        }
    }

    /**
     * 실행 위임에 실패한 작업을 즉시 HOLDING으로 돌려 다음 폴링에서 다시 선점할 수 있게 한다.
     *
     * 롤백 UPDATE 자체가 던져도 삼킨다. 여기서 예외가 올라가면 permit 반납 경로까지 함께 무너지는데,
     * 롤백에 실패한 작업은 PROCESSING에 남더라도 DeadJobSchedulerTask의 timeout 회수 경로가 되돌려준다.
     */
    private void rollbackToHolding(Job job) {
        try {
            jobRepository.transitionIfStatusAndAttemptMatch(
                    job.getId(), JobStatus.HOLDING, JobStatus.PROCESSING, job.getAttemptNo(), Instant.now());
        } catch (RuntimeException e) {
            log.error("선점 롤백 실패, timeout 회수 대기: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo(), e);
        }
    }

    /** 외부 작업 종료 시 반드시 실행 슬롯을 반납한다. */
    private void processAndRelease(Job job) {
        try {
            jobProcessor.process(job);
        } finally {
            availableWorkers.release();
        }
    }
}
