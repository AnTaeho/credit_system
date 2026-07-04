package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class ConfirmServiceTest {

    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;

    ConfirmService confirmService;

    @BeforeEach
    void setUp() {
        confirmService = new ConfirmService(jobRepository, ledgerRepository);
    }

    @Test
    void attemptNo가_일치하면_완료_처리되고_ledger가_남는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        confirmService.confirm(job.getId(), 0, "https://stub/x.png");

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(found.getResultUrl()).isEqualTo("https://stub/x.png");
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(1L)).hasSize(1);
    }

    @Test
    void attemptNo가_불일치하면_아무것도_하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        confirmService.confirm(job.getId(), 5, "https://stub/x.png");

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.HOLDING);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(1L)).isEmpty();
    }
}
