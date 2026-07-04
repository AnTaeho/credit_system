package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.outbox.service.OutboxWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class RetryService {

    private final JobRepository jobRepository;
    private final OutboxWriter outboxWriter;

    @Transactional
    public void retry(Job job) {
        int updated = jobRepository.incrementAttemptForRetry(job.getId(), job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("재시도 투입 경쟁에서 밀림 또는 이미 처리됨: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return;
        }
        int newAttemptNo = job.getAttemptNo() + 1;
        outboxWriter.write(job.getId(), job.getOrganizationId(), newAttemptNo, job.getPrompt());
        log.info("재시도 투입: jobId={}, newAttemptNo={}", job.getId(), newAttemptNo);
    }
}
