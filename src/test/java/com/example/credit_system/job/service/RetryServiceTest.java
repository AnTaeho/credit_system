package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.outbox.repository.OutboxRepository;
import com.example.credit_system.outbox.service.OutboxWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class RetryServiceTest {

    @Autowired JobRepository jobRepository;
    @Autowired OutboxRepository outboxRepository;

    RetryService retryService;

    @BeforeEach
    void setUp() {
        OutboxWriter outboxWriter = new OutboxWriter(outboxRepository, new ObjectMapper());
        retryService = new RetryService(jobRepository, outboxWriter);
    }

    @Test
    void FAILED_상태의_job은_attemptNo가_증가하고_outbox가_재발행된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.updateStatusIfAttemptMatches(job.getId(), JobStatus.FAILED, 0, Instant.now());

        retryService.retry(jobRepository.findById(job.getId()).orElseThrow());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.PROCESSING);
        assertThat(found.getAttemptNo()).isEqualTo(1);
        assertThat(outboxRepository.findBySentFalseOrderByIdAsc()).hasSize(1);
    }

    @Test
    void FAILED_상태가_아니면_아무것도_하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        retryService.retry(job);

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getAttemptNo()).isZero();
        assertThat(outboxRepository.findBySentFalseOrderByIdAsc()).isEmpty();
    }
}
