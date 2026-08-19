package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.JobLifecycleService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.scheduling", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DeadJobSchedulerTask {

    private static final int SCAN_BATCH_SIZE = 100;

    private final HeartbeatRegistry heartbeatRegistry;
    private final JobRepository jobRepository;
    private final JobLifecycleService jobLifecycleService;
    private final AppProperties appProperties;

    @Scheduled(fixedDelayString = "${app.scheduling.dead-job-scan-interval-millis:5000}")
    public void scan() {
        try {
            markExpiredJobsAsFailed();
        } catch (RuntimeException e) {
            log.error("heartbeat 만료 회수 단계 실패, 이번 주기 건너뜀", e);
        }
        try {
            reapStaleProcessing();
        } catch (RuntimeException e) {
            log.error("PROCESSING 정체 회수 단계 실패, 이번 주기 건너뜀", e);
        }
        try {
            processFailedJobs();
        } catch (RuntimeException e) {
            log.error("FAILED job 재검토 단계 실패, 이번 주기 건너뜀", e);
        }
    }

    private void markExpiredJobsAsFailed() {
        for (Long jobId : heartbeatRegistry.findExpiredJobIds()) {
            try {
                jobRepository.findById(jobId).ifPresent(job -> {
                    int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                            jobId, JobStatus.FAILED, JobStatus.PROCESSING, job.getAttemptNo(), Instant.now());
                    if (updated == 1) {
                        log.info("heartbeat 만료로 FAILED 전이: jobId={}, attemptNo={}", jobId, job.getAttemptNo());
                    }
                });
                heartbeatRegistry.remove(jobId);
            } catch (RuntimeException e) {
                log.warn("heartbeat 만료 job 회수 실패: jobId={}", jobId, e);
            }
        }
    }

    private void reapStaleProcessing() {
        Instant cutoff = Instant.now().minusSeconds(appProperties.processing().timeoutSeconds());
        for (Job job : jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(
                JobStatus.PROCESSING, cutoff, PageRequest.of(0, SCAN_BATCH_SIZE))) {
            try {
                if (heartbeatRegistry.hasLiveHeartbeat(job.getId())) {
                    continue;
                }
                int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                        job.getId(), JobStatus.FAILED, JobStatus.PROCESSING, job.getAttemptNo(), Instant.now());
                if (updated == 1) {
                    heartbeatRegistry.remove(job.getId());
                    log.info("PROCESSING 정체 job 회수, FAILED 전이: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
                }
            } catch (RuntimeException e) {
                log.warn("PROCESSING 정체 job 회수 실패: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo(), e);
            }
        }
    }

    private void processFailedJobs() {
        for (Job job : jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED, PageRequest.of(0, SCAN_BATCH_SIZE))) {
            try {
                if (job.getAttemptNo() + 1 < appProperties.generation().maxAttempts()) {
                    jobLifecycleService.retry(job);
                } else {
                    jobLifecycleService.finalRefund(job);
                }
            } catch (RuntimeException e) {
                log.warn("FAILED job 재검토 실패: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo(), e);
            }
        }
    }
}
