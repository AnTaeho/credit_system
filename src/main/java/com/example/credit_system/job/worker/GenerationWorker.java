package com.example.credit_system.job.worker;

import com.example.credit_system.global.config.WorkerProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@ConditionalOnExpression("${app.scheduling.enabled:true} and ${app.worker.enabled:true}")
public class GenerationWorker {

    private final JobRepository jobRepository;
    private final GenerationJobProcessor jobProcessor;
    private final TaskExecutor workerExecutor;
    private final int batchSize;

    public GenerationWorker(JobRepository jobRepository,
                            GenerationJobProcessor jobProcessor,
                            @Qualifier("generationWorkerExecutor") TaskExecutor workerExecutor,
                            WorkerProperties workerProperties) {
        this.jobRepository = jobRepository;
        this.jobProcessor = jobProcessor;
        this.workerExecutor = workerExecutor;
        this.batchSize = workerProperties.batchSize();
    }

    @Scheduled(fixedDelayString = "${app.scheduling.worker-interval-millis:500}")
    public void processPendingJobs() {
        List<Job> jobs = jobRepository.findByStatusOrderByIdAsc(JobStatus.HOLDING, PageRequest.of(0, batchSize));
        for (Job job : jobs) {
            if (!claim(job)) {
                continue;
            }
            if (!dispatch(job)) {
                return;
            }
        }
    }

    private boolean claim(Job job) {
        try {
            int updated = jobRepository.startProcessingIfAttemptMatches(
                    job.getId(), job.getAttemptNo(), Instant.now());
            if (updated == 0) {
                log.info("다른 워커가 선점했거나 무효한 작업 무시: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
                return false;
            }
            return true;
        } catch (RuntimeException e) {
            log.warn("생성 작업 선점 실패: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo(), e);
            return false;
        }
    }

    private boolean dispatch(Job job) {
        try {
            workerExecutor.execute(() -> jobProcessor.process(job));
            return true;
        } catch (RuntimeException e) {
            log.warn("생성 작업 executor 위임 실패, 이번 주기 중단: jobId={}, attemptNo={}",
                    job.getId(), job.getAttemptNo(), e);
            rollbackToHolding(job);
            return false;
        }
    }

    private void rollbackToHolding(Job job) {
        try {
            jobRepository.transitionIfStatusAndAttemptMatch(
                    job.getId(), JobStatus.HOLDING, JobStatus.PROCESSING, job.getAttemptNo(), Instant.now());
        } catch (RuntimeException e) {
            log.error("선점 롤백 실패, timeout 회수 대기: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo(), e);
        }
    }
}
