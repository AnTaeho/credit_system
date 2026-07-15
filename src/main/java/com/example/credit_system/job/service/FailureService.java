package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class FailureService {

    private final JobRepository jobRepository;

    /** 유효한 작업 시도를 실패 상태로 변경한다. */
    @Transactional
    public void markFailed(Long jobId, int attemptNo) {
        int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                jobId, JobStatus.FAILED, JobStatus.PROCESSING, attemptNo, Instant.now());
        if (updated == 0) {
            log.info("이미 무효화된 시도, 실패 처리 무시: jobId={}, attemptNo={}", jobId, attemptNo);
            return;
        }
        log.info("실패 처리: jobId={}, attemptNo={}", jobId, attemptNo);
    }
}
