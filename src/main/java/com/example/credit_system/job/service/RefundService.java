package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
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

    private static final int MAX_LOCK_RETRIES = 3;

    private final JobRepository jobRepository;
    private final OrganizationRepository organizationRepository;
    private final LedgerRepository ledgerRepository;

    @Transactional
    public void finalRefund(Job job) {
        int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.FAILED, job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("이미 늦은 워커가 처리함, 환불 취소: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return;
        }

        for (int attempt = 0; attempt < MAX_LOCK_RETRIES; attempt++) {
            Organization organization = organizationRepository.findById(job.getOrganizationId())
                    .orElseThrow(() -> new IllegalStateException("존재하지 않는 organization: " + job.getOrganizationId()));

            int orgUpdated = organizationRepository.addBalance(
                    job.getOrganizationId(), job.getHoldAmount(), organization.getVersion(), Instant.now());
            if (orgUpdated == 1) {
                ledgerRepository.save(LedgerEntry.of(job.getOrganizationId(), job.getId(), LedgerType.REFUND, job.getHoldAmount()));
                log.info("최종 환불 완료: jobId={}, organizationId={}, amount={}",
                        job.getId(), job.getOrganizationId(), job.getHoldAmount());
                return;
            }
            log.info("환불 중 잔액 버전 충돌, 재시도: organizationId={}, attempt={}", job.getOrganizationId(), attempt);
        }
        log.error("환불 잔액 반영이 반복 충돌로 실패함 - 운영 알림 필요: jobId={}, organizationId={}, amount={}",
                job.getId(), job.getOrganizationId(), job.getHoldAmount());
    }
}
