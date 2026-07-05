package com.example.credit_system.job.service;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.exception.DuplicateRequestInProgressException;
import com.example.credit_system.global.exception.InsufficientBalanceException;
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

    @Transactional
    public HoldResult requestGeneration(Long organizationId, String idemKey, String prompt) {
        Optional<IdempotencyKey> existing = idempotencyKeyRepository
                .findByOrganizationIdAndIdemKey(organizationId, idemKey);
        if (existing.isPresent()) {
            return toDuplicateResult(existing.get());
        }

        // 여기서 saveAndFlush로 예외를 캐치해 처리하지 않는다: Hibernate는 flush 중 예외가 발생한 세션을
        // 계속 사용하면(같은 트랜잭션에서 추가 쿼리를 날리면) AssertionFailure로 죽어버린다. 위에서 먼저
        // SELECT로 대부분의 중복 요청을 걸러내고, 극히 드문 동시 경합(두 요청이 동시에 SELECT를 통과)만
        // unique 제약 위반으로 트랜잭션 전체가 롤백되게 두고, GlobalExceptionHandler가 이를 번역한다.
        idempotencyKeyRepository.save(new IdempotencyKey(organizationId, idemKey));

        long cost = appProperties.generation().cost();

        // TODO : 리팩토링 필요할듯 SRP
        Job job = deductBalanceAndCreateJob(organizationId, cost, prompt);

        idempotencyKeyRepository.attachJobId(organizationId, idemKey, job.getId());
        ledgerRepository.save(LedgerEntry.of(organizationId, job.getId(), LedgerType.HOLD, -cost));
        outboxWriter.write(job.getId(), organizationId, job.getAttemptNo(), prompt);

        log.info("hold 완료: organizationId={}, jobId={}, cost={}", organizationId, job.getId(), cost);
        return new HoldResult(job.getId(), false);
    }

    private HoldResult toDuplicateResult(IdempotencyKey existing) {
        if (existing.getJobId() == null) {
            throw new DuplicateRequestInProgressException();
        }
        log.info("중복 요청 감지: jobId={}", existing.getJobId());
        return new HoldResult(existing.getJobId(), true);
    }

    private Job deductBalanceAndCreateJob(Long organizationId, long cost, String prompt) {
        Organization organization = organizationRepository.findById(organizationId)
                .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 organization: " + organizationId));

        if (organization.getBalance() < cost) {
            throw new InsufficientBalanceException(organization.getBalance(), cost);
        }

        int updated = organizationRepository.deductBalance(organizationId, cost, Instant.now());
        if (updated == 1) {
            return jobRepository.save(Job.hold(organizationId, cost, prompt));
        }
        throw new InsufficientBalanceException(organization.getBalance(), cost);
    }
}
