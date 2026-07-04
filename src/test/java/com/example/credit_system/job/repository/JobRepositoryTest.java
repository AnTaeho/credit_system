package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class JobRepositoryTest {

    @Autowired
    JobRepository jobRepository;

    @Test
    void attemptNo가_일치하면_상태가_변경된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int updated = jobRepository.updateStatusIfAttemptMatches(
                job.getId(), JobStatus.PROCESSING, 0, Instant.now());

        assertThat(updated).isEqualTo(1);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.PROCESSING);
    }

    @Test
    void attemptNo가_불일치하면_0행이며_상태가_유지된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int updated = jobRepository.updateStatusIfAttemptMatches(
                job.getId(), JobStatus.PROCESSING, 5, Instant.now());

        assertThat(updated).isZero();
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.HOLDING);
    }

    @Test
    void 완료전이는_resultUrl을_함께_기록한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int updated = jobRepository.completeIfAttemptMatches(
                job.getId(), "https://stub/image/1.png", 0, Instant.now());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(found.getResultUrl()).isEqualTo("https://stub/image/1.png");
    }

    @Test
    void 상태와_attemptNo가_모두_일치할_때만_전이된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.updateStatusIfAttemptMatches(job.getId(), JobStatus.FAILED, 0, Instant.now());

        int wrongStatus = jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.COMPLETED, 0, Instant.now());
        int match = jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.FAILED, 0, Instant.now());

        assertThat(wrongStatus).isZero();
        assertThat(match).isEqualTo(1);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.REFUNDED);
    }

    @Test
    void 재시도_투입은_FAILED_상태에서만_attemptNo를_증가시킨다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int beforeFail = jobRepository.incrementAttemptForRetry(job.getId(), 0, Instant.now());
        assertThat(beforeFail).isZero(); // 아직 HOLDING이므로 실패

        jobRepository.updateStatusIfAttemptMatches(job.getId(), JobStatus.FAILED, 0, Instant.now());
        int afterFail = jobRepository.incrementAttemptForRetry(job.getId(), 0, Instant.now());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(afterFail).isEqualTo(1);
        assertThat(found.getStatus()).isEqualTo(JobStatus.PROCESSING);
        assertThat(found.getAttemptNo()).isEqualTo(1);
    }
}
