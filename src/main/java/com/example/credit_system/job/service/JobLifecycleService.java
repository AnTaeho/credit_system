package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobLifecycleService {

    private final JobRepository jobRepository;
    private final OrganizationRepository organizationRepository;
    private final LedgerRepository ledgerRepository;

    @Transactional
    public void confirm(Job job, String resultUrl) {
        int updated = jobRepository.completeIfAttemptMatches(job.getId(), resultUrl, job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("이미 무효화된 시도, confirm 무시: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return;
        }
        ledgerRepository.save(LedgerEntry.of(job.getOrganizationId(), job.getId(), LedgerType.CONFIRM, 0));
        log.info("confirm 완료: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
    }

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

    @Transactional
    public void finalRefund(Job job) {
        int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.FAILED, job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("이미 늦은 워커가 처리함, 환불 취소: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return;
        }

        int orgUpdated = organizationRepository.addBalance(
                job.getOrganizationId(), job.getHoldAmount(), Instant.now());
        if (orgUpdated == 1) {
            ledgerRepository.save(LedgerEntry.of(job.getOrganizationId(), job.getId(), LedgerType.REFUND, job.getHoldAmount()));
            log.info("최종 환불 완료: jobId={}, organizationId={}, amount={}",
                    job.getId(), job.getOrganizationId(), job.getHoldAmount());
            return;
        }
        throw new IllegalStateException("환불 잔액 반영 실패: organization이 존재하지 않음, jobId=" + job.getId()
                + ", organizationId=" + job.getOrganizationId());
    }
}
