package com.example.credit_system.global.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.RefundService;
import com.example.credit_system.job.service.RetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.scheduling", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DeadJobSchedulerTask {

    private final HeartbeatRegistry heartbeatRegistry;
    private final JobRepository jobRepository;
    private final RetryService retryService;
    private final RefundService refundService;
    private final AppProperties appProperties;

    /** 만료되거나 실패한 작업을 찾아 재시도 또는 환불한다. */
    @Scheduled(fixedDelayString = "${app.scheduling.dead-job-scan-interval-millis:5000}")
    public void scan() {
        for (Long jobId : heartbeatRegistry.findExpiredJobIds()) {
            markExpiredAsFailed(jobId);
        }
        reapStaleHolding();
        reapStaleProcessing();
        for (Job job : jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)) {
            process(job);
        }
    }

    /** 오래 정체된 HOLDING 작업을 실패 상태로 회수한다. */
    private void reapStaleHolding() {
        Instant cutoff = Instant.now().minusSeconds(appProperties.holding().timeoutSeconds());
        for (Job job : jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(JobStatus.HOLDING, cutoff)) {
            int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                    job.getId(), JobStatus.FAILED, JobStatus.HOLDING, job.getAttemptNo(), Instant.now());
            if (updated == 1) {
                log.info("HOLDING 정체 job 회수, FAILED 전이: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            }
        }
    }

    /** heartbeat가 없는 오래된 PROCESSING 작업을 실패 상태로 회수한다. */
    private void reapStaleProcessing() {
        Instant cutoff = Instant.now().minusSeconds(appProperties.processing().timeoutSeconds());
        for (Job job : jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(JobStatus.PROCESSING, cutoff)) {
            if (heartbeatRegistry.hasLiveHeartbeat(job.getId())) {
                continue;
            }
            int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                    job.getId(), JobStatus.FAILED, JobStatus.PROCESSING, job.getAttemptNo(), Instant.now());
            if (updated == 1) {
                heartbeatRegistry.remove(job.getId());
                log.info("PROCESSING 정체 job 회수, FAILED 전이: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            }
        }
    }

    /** heartbeat가 만료된 작업을 실패 상태로 변경한다. */
    private void markExpiredAsFailed(Long jobId) {
        jobRepository.findById(jobId).ifPresent(job -> {
            int updated = jobRepository.updateStatusIfAttemptMatches(
                    jobId, JobStatus.FAILED, job.getAttemptNo(), Instant.now());
            if (updated == 1) {
                heartbeatRegistry.remove(jobId);
                log.info("heartbeat 만료로 FAILED 전이: jobId={}, attemptNo={}", jobId, job.getAttemptNo());
            }
        });
    }

    /** 최신 실패 상태를 확인해 재시도 또는 환불을 수행한다. */
    private void process(Job job) {
        Job current = jobRepository.findById(job.getId()).orElse(null);
        if (current == null || current.getStatus() != JobStatus.FAILED) {
            return;
        }
        if (current.getAttemptNo() < appProperties.generation().maxAttempts()) {
            retryService.retry(current);
        } else {
            refundService.finalRefund(current);
        }
    }
}
