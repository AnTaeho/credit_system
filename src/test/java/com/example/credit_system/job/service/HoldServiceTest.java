package com.example.credit_system.job.service;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.exception.InsufficientBalanceException;
import com.example.credit_system.job.repository.IdempotencyKeyRepository;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.outbox.repository.OutboxRepository;
import com.example.credit_system.outbox.service.OutboxWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ActiveProfiles("test")
@DataJpaTest
class HoldServiceTest {

    @Autowired IdempotencyKeyRepository idempotencyKeyRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired OutboxRepository outboxRepository;

    HoldService holdService;
    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 1000L));
        AppProperties appProperties = new AppProperties(
                new AppProperties.Generation(100L, 3), null, null, null, null, null);
        OutboxWriter outboxWriter = new OutboxWriter(outboxRepository, new ObjectMapper());
        holdService = new HoldService(idempotencyKeyRepository, organizationRepository,
                jobRepository, ledgerRepository, outboxWriter, appProperties);
    }

    @Test
    void 정상_요청은_잔액을_차감하고_job과_ledger와_outbox를_생성한다() {
        HoldResult result = holdService.requestGeneration(organization.getId(), "key-1", "a cat");

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(result.duplicate()).isFalse();
        assertThat(found.getBalance()).isEqualTo(900L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).hasSize(1);
        assertThat(outboxRepository.findBySentFalseOrderByIdAsc()).hasSize(1);
    }

    @Test
    void 동일_idemKey로_재요청하면_같은_job을_반환하고_잔액이_추가로_차감되지_않는다() {
        HoldResult first = holdService.requestGeneration(organization.getId(), "key-1", "a cat");
        HoldResult second = holdService.requestGeneration(organization.getId(), "key-1", "a cat");

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(second.duplicate()).isTrue();
        assertThat(second.jobId()).isEqualTo(first.jobId());
        assertThat(found.getBalance()).isEqualTo(900L);
    }

    @Test
    void 잔액이_부족하면_예외가_발생하고_job이_생성되지_않는다() {
        Organization poor = organizationRepository.save(new Organization("poor", 50L));

        assertThatThrownBy(() -> holdService.requestGeneration(poor.getId(), "key-2", "a cat"))
                .isInstanceOf(InsufficientBalanceException.class);

        assertThat(jobRepository.findByOrganizationIdOrderByIdDesc(poor.getId())).isEmpty();
    }
}
