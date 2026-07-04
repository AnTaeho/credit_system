package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConfirmService {

    private final JobRepository jobRepository;
    private final LedgerRepository ledgerRepository;

    @Transactional
    public void confirm(Long jobId, int attemptNo, String resultUrl) {
        int updated = jobRepository.completeIfAttemptMatches(jobId, resultUrl, attemptNo, Instant.now());
        if (updated == 0) {
            log.info("이미 무효화된 시도, confirm 무시: jobId={}, attemptNo={}", jobId, attemptNo);
            return;
        }
        Job job = jobRepository.findById(jobId).orElseThrow();
        ledgerRepository.save(LedgerEntry.of(job.getOrganizationId(), jobId, LedgerType.CONFIRM, 0));
        log.info("confirm 완료: jobId={}, attemptNo={}", jobId, attemptNo);
    }
}
