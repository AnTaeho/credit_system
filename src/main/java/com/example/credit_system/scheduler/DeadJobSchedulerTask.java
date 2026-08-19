package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.JobLifecycleService;
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
    private final JobLifecycleService jobLifecycleService;
    private final AppProperties appProperties;

    @Scheduled(fixedDelayString = "${app.scheduling.dead-job-scan-interval-millis:5000}")
    public void scan() {
        for (Long jobId : heartbeatRegistry.findExpiredJobIds()) {
            markExpiredAsFailed(jobId);
        }
        reapStaleProcessing();
        for (Job job : jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)) {
            process(job);
        }
    }

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

    private void markExpiredAsFailed(Long jobId) {
        jobRepository.findById(jobId).ifPresentOrElse(job -> {
            int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                    jobId, JobStatus.FAILED, JobStatus.PROCESSING, job.getAttemptNo(), Instant.now());
            if (updated == 1) {
                log.info("heartbeat 만료로 FAILED 전이: jobId={}, attemptNo={}", jobId, job.getAttemptNo());
            }
            heartbeatRegistry.remove(jobId);
        }, () -> heartbeatRegistry.remove(jobId));
    }

    private void process(Job job) {
        Job current = jobRepository.findById(job.getId()).orElse(null);
        if (current == null || current.getStatus() != JobStatus.FAILED) {
            return;
        }
        if (current.getAttemptNo() + 1 < appProperties.generation().maxAttempts()) {
            jobLifecycleService.retry(current);
        } else {
            jobLifecycleService.finalRefund(current);
        }
    }
}
