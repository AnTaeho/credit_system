package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.repository.JobRepository;
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

    /** 실패한 작업의 시도 번호를 높여 DB 작업 큐에 다시 대기시킨다. */
    @Transactional
    public void retry(Job job) {
        int updated = jobRepository.incrementAttemptForRetry(job.getId(), job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("재시도 투입 경쟁에서 밀림 또는 이미 처리됨: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return;
        }
        int newAttemptNo = job.getAttemptNo() + 1;
        log.info("재시도 투입: jobId={}, newAttemptNo={}", job.getId(), newAttemptNo);
    }
}
