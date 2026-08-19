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
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class JobLifecycleServiceTest {

    @Autowired JobRepository jobRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    JobLifecycleService jobLifecycleService;

    @BeforeEach
    void setUp() {
        jobLifecycleService = new JobLifecycleService(jobRepository, organizationRepository, ledgerRepository);
    }

    @Test
    void attemptNo가_일치하면_완료_처리되고_ledger가_남는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());

        jobLifecycleService.confirm(job, "https://stub/x.png");

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(found.getResultUrl()).isEqualTo("https://stub/x.png");
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(1L)).hasSize(1);
    }

    @Test
    void attemptNo가_불일치하면_아무것도_하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());
        ReflectionTestUtils.setField(job, "attemptNo", 5);

        jobLifecycleService.confirm(job, "https://stub/x.png");

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.PROCESSING);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(1L)).isEmpty();
    }

    @Test
    void 환불된_작업의_늦은_confirm은_무시한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, java.time.Instant.now());
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.FAILED, 0, java.time.Instant.now());

        jobLifecycleService.confirm(job, "https://stub/late.png");

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.REFUNDED);
        assertThat(found.getResultUrl()).isNull();
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(1L)).isEmpty();
    }

    @Test
    void 같은_attempt의_confirm을_두_번_호출해도_원장은_한_번만_기록된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());

        jobLifecycleService.confirm(job, "https://stub/first.png");
        jobLifecycleService.confirm(job, "https://stub/second.png");

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(found.getResultUrl()).isEqualTo("https://stub/first.png");
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(1L)).hasSize(1);
    }

    @Test
    void attemptNo가_일치하면_FAILED로_전이한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());

        jobLifecycleService.markFailed(job.getId(), 0);

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    void attemptNo가_불일치하면_전이하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());

        jobLifecycleService.markFailed(job.getId(), 9);

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.PROCESSING);
    }

    @Test
    void 완료된_작업의_늦은_실패는_무시한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());
        jobRepository.completeIfAttemptMatches(
                job.getId(), "https://stub/done.png", 0, java.time.Instant.now());

        jobLifecycleService.markFailed(job.getId(), 0);

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.COMPLETED);
    }

    @Test
    void FAILED_상태의_job은_attemptNo가_증가하고_다시_대기한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());

        jobLifecycleService.retry(jobRepository.findById(job.getId()).orElseThrow());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.HOLDING);
        assertThat(found.getAttemptNo()).isEqualTo(1);
    }

    @Test
    void FAILED_상태가_아니면_아무것도_하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        jobLifecycleService.retry(job);

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getAttemptNo()).isZero();
    }

    @Test
    void FAILED_job은_REFUNDED로_전이되고_잔액이_복구된다() {
        Organization organization = organizationRepository.save(new Organization("acme", 700L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 300L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());

        jobLifecycleService.finalRefund(jobRepository.findById(job.getId()).orElseThrow());

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

        jobLifecycleService.finalRefund(job);

        Organization foundOrg = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(foundOrg.getBalance()).isEqualTo(700L);
    }

    @Test
    void 같은_작업을_두_번_환불해도_잔액과_원장은_한_번만_반영된다() {
        Organization organization = organizationRepository.save(new Organization("acme", 700L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 300L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());
        Job failed = jobRepository.findById(job.getId()).orElseThrow();

        jobLifecycleService.finalRefund(failed);
        jobLifecycleService.finalRefund(failed);

        assertThat(organizationRepository.findById(organization.getId()).orElseThrow().getBalance())
                .isEqualTo(1000L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .hasSize(1);
    }
}
