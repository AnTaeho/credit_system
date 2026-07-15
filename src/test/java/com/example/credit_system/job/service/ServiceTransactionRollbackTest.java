package com.example.credit_system.job.service;

import com.example.credit_system.global.exception.InsufficientBalanceException;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.IdempotencyKeyRepository;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.outbox.repository.OutboxRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ActiveProfiles("test")
@SpringBootTest
class ServiceTransactionRollbackTest {

    @Autowired HoldService holdService;
    @Autowired RefundService refundService;
    @Autowired IdempotencyKeyRepository idempotencyKeyRepository;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired OutboxRepository outboxRepository;
    @Autowired OrganizationRepository organizationRepository;

    @AfterEach
    void tearDown() {
        outboxRepository.deleteAll();
        ledgerRepository.deleteAll();
        idempotencyKeyRepository.deleteAll();
        jobRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 잔액_부족으로_hold가_실패하면_선점한_멱등_키도_롤백된다() {
        Organization organization = organizationRepository.save(new Organization("poor", 50L));

        assertThatThrownBy(() -> holdService.requestGeneration(
                organization.getId(), "rollback-key", "cat"))
                .isInstanceOf(InsufficientBalanceException.class);

        assertThat(idempotencyKeyRepository.findByOrganizationIdAndIdemKey(
                organization.getId(), "rollback-key")).isEmpty();
        assertThat(jobRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).isEmpty();
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).isEmpty();
        assertThat(outboxRepository.findBySentFalseOrderByIdAsc()).isEmpty();
        assertThat(organizationRepository.findById(organization.getId()).orElseThrow().getBalance())
                .isEqualTo(50L);
    }

    @Test
    void 환불할_조직이_사라졌으면_REFUNDED_전이도_롤백된다() {
        Organization organization = organizationRepository.save(new Organization("deleted", 0L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 100L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());
        Job failed = jobRepository.findById(job.getId()).orElseThrow();
        organizationRepository.deleteById(organization.getId());

        assertThatThrownBy(() -> refundService.finalRefund(failed))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("환불 잔액 반영 실패");

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.FAILED);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).isEmpty();
    }
}
