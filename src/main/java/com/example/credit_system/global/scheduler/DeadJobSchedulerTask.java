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

    @Scheduled(fixedDelayString = "${app.scheduling.dead-job-scan-interval-millis:5000}")
    public void scan() {
        for (Long jobId : heartbeatRegistry.findExpiredJobIds()) {
            markExpiredAsFailed(jobId);
        }
        reapStaleHolding();
        for (Job job : jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)) {
            process(job);
        }
    }

    // outbox 발행 실패(브로커 장애 등)로 메시지가 유실되면 HOLDING에서 영구 정체될 수 있다.
    // 일정 시간 이상 갱신되지 않은 HOLDING job을 FAILED로 되돌려 기존 FAILED 처리 루프가
    // 재시도/환불을 이어받게 한다. status+attemptNo 이중 fencing으로, 그 사이 outbox가
    // 뒤늦게 발행되어 컨슈머가 PROCESSING으로 이미 가져갔다면 이 전이는 무효(0행)가 된다.
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

    private void process(Job job) {
        // scan() 시작 시점의 스냅샷일 수 있으므로 재조회해 최신 상태로 분기한다.
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
