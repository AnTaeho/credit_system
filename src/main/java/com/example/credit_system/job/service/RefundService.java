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
public class RefundService {

    private final JobRepository jobRepository;
    private final OrganizationRepository organizationRepository;
    private final LedgerRepository ledgerRepository;

    /** 최종 실패 작업을 환불하고 원장에 기록한다. */
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
