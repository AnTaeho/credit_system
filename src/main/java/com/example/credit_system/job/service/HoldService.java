package com.example.credit_system.job.service;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.exception.DuplicateRequestInProgressException;
import com.example.credit_system.global.exception.InsufficientBalanceException;
import com.example.credit_system.global.exception.InvalidRequestException;
import com.example.credit_system.job.domain.IdempotencyKey;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.repository.IdempotencyKeyRepository;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.outbox.service.OutboxWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class HoldService {

    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final OrganizationRepository organizationRepository;
    private final JobRepository jobRepository;
    private final LedgerRepository ledgerRepository;
    private final OutboxWriter outboxWriter;
    private final AppProperties appProperties;

    /** 멱등성을 보장하며 비용을 차감하고 생성 작업을 등록한다. */
    @Transactional
    public HoldResult requestGeneration(Long organizationId, String idemKey, String prompt) {
        validateRequest(idemKey, prompt);

        Optional<IdempotencyKey> existing = idempotencyKeyRepository
                .findByOrganizationIdAndIdemKey(organizationId, idemKey);
        if (existing.isPresent()) {
            return toDuplicateResult(existing.get());
        }

        idempotencyKeyRepository.save(new IdempotencyKey(organizationId, idemKey));

        long cost = appProperties.generation().cost();

        deductBalance(organizationId, cost);
        Job job = jobRepository.save(Job.hold(organizationId, cost, prompt));

        int attached = idempotencyKeyRepository.attachJobId(organizationId, idemKey, job.getId());
        if (attached != 1) {
            throw new IllegalStateException("idempotency key에 jobId 연결 실패: organizationId=" + organizationId
                    + ", idemKey=" + idemKey + ", jobId=" + job.getId());
        }
        ledgerRepository.save(LedgerEntry.of(organizationId, job.getId(), LedgerType.HOLD, -cost));
        outboxWriter.write(job.getId(), organizationId, job.getAttemptNo(), prompt);

        log.info("hold 완료: organizationId={}, jobId={}, cost={}", organizationId, job.getId(), cost);
        return new HoldResult(job.getId(), false);
    }

    /** 생성 요청 값이 저장 가능한 범위인지 검증한다. */
    private void validateRequest(String idemKey, String prompt) {
        if (idemKey == null || idemKey.isBlank()) {
            throw new InvalidRequestException("idemKey는 필수입니다.");
        }
        if (idemKey.length() > 100) {
            throw new InvalidRequestException("idemKey는 100자를 초과할 수 없습니다.");
        }
        if (prompt == null || prompt.isBlank()) {
            throw new InvalidRequestException("prompt는 필수입니다.");
        }
        if (prompt.length() > 1000) {
            throw new InvalidRequestException("prompt는 1000자를 초과할 수 없습니다.");
        }
    }

    /** 기존 멱등 키를 중복 요청 결과로 변환한다. */
    private HoldResult toDuplicateResult(IdempotencyKey existing) {
        if (existing.getJobId() == null) {
            throw new DuplicateRequestInProgressException();
        }
        log.info("중복 요청 감지: jobId={}", existing.getJobId());
        return new HoldResult(existing.getJobId(), true);
    }

    /** 잔액이 충분하면 비용을 원자적으로 차감한다. */
    private void deductBalance(Long organizationId, long cost) {
        int updated = organizationRepository.deductBalance(organizationId, cost, Instant.now());
        if (updated == 1) {
            return;
        }

        Organization organization = organizationRepository.findById(organizationId)
                .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 organization: " + organizationId));
        throw new InsufficientBalanceException(organization.getBalance(), cost);
    }
}
