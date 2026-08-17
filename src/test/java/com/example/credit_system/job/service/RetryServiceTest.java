package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
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

    RetryService retryService;

    @BeforeEach
    void setUp() {
        retryService = new RetryService(jobRepository);
    }

    @Test
    void FAILED_상태의_job은_attemptNo가_증가하고_다시_대기한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());

        retryService.retry(jobRepository.findById(job.getId()).orElseThrow());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.HOLDING);
        assertThat(found.getAttemptNo()).isEqualTo(1);
    }

    @Test
    void FAILED_상태가_아니면_아무것도_하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        retryService.retry(job);

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getAttemptNo()).isZero();
    }
}
