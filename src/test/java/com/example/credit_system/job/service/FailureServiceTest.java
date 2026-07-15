package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class FailureServiceTest {

    @Autowired JobRepository jobRepository;

    FailureService failureService;

    @BeforeEach
    void setUp() {
        failureService = new FailureService(jobRepository);
    }

    @Test
    void attemptNo가_일치하면_FAILED로_전이한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());

        failureService.markFailed(job.getId(), 0);

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    void attemptNo가_불일치하면_전이하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());

        failureService.markFailed(job.getId(), 9);

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.PROCESSING);
    }

    @Test
    void 완료된_작업의_늦은_실패는_무시한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, java.time.Instant.now());
        jobRepository.completeIfAttemptMatches(
                job.getId(), "https://stub/done.png", 0, java.time.Instant.now());

        failureService.markFailed(job.getId(), 0);

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.COMPLETED);
    }
}
