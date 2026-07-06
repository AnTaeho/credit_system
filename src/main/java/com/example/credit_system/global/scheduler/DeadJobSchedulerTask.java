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
        reapStaleProcessing();
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

    // PROCESSING 진입 직후 heartbeat 등록 전에 워커가 크래시하거나, 등록 후 Redis 데이터가
    // 유실되거나, DLT(Dead Letter Topic)로 격리된 메시지의 job이 PROCESSING에 남는 경우
    // 스케줄러의 heartbeat 만료 감지(findExpiredJobIds)로는 회수할 수 없다. 이를 보완해
    // 일정 시간 이상 갱신되지 않은 PROCESSING job 중 Redis에 살아있는 heartbeat가 없는
    // 것만 FAILED로 되돌린다. status+attemptNo 이중 fencing 덕에, 실제로는 워커가 정상
    // 동작 중인데 오탐(hasLiveHeartbeat 조회 시점의 간극 등)이 나더라도 이후 retry가
    // attemptNo를 증가시키므로 옛 워커의 최종 confirm(complete/updateStatusIfAttemptMatches)은
    // attemptNo 불일치로 0행이 되어 무효화되어 안전하다.
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
