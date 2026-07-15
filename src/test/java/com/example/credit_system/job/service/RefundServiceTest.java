package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class RefundServiceTest {

    @Autowired JobRepository jobRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    RefundService refundService;

    @BeforeEach
    void setUp() {
        refundService = new RefundService(jobRepository, organizationRepository, ledgerRepository);
    }

    @Test
    void FAILED_job은_REFUNDED로_전이되고_잔액이_복구된다() {
        Organization organization = organizationRepository.save(new Organization("acme", 700L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 300L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());

        refundService.finalRefund(jobRepository.findById(job.getId()).orElseThrow());

        Job foundJob = jobRepository.findById(job.getId()).orElseThrow();
        Organization foundOrg = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(foundJob.getStatus()).isEqualTo(JobStatus.REFUNDED);
        assertThat(foundOrg.getBalance()).isEqualTo(1000L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .anyMatch(entry -> entry.getType().name().equals("REFUND"));
    }

    @Test
    void FAILED_상태가_아니면_환불하지_않는다() {
        Organization organization = organizationRepository.save(new Organization("acme", 700L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 300L, "cat"));

        refundService.finalRefund(job);

        Organization foundOrg = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(foundOrg.getBalance()).isEqualTo(700L);
    }
}
