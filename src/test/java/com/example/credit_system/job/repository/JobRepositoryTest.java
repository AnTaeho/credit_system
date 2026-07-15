package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class JobRepositoryTest {

    @Autowired
    JobRepository jobRepository;

    @Test
    void 대기_작업은_attemptNo가_일치하면_처리_상태로_전이된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int updated = jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, Instant.now());

        assertThat(updated).isEqualTo(1);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.PROCESSING);
    }

    @Test
    void attemptNo가_불일치하면_0행이며_상태가_유지된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int updated = jobRepository.startProcessingIfAttemptMatches(job.getId(), 5, Instant.now());

        assertThat(updated).isZero();
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.HOLDING);
    }

    @Test
    void 재시도에서_미리_PROCESSING된_작업도_같은_attemptNo면_처리할_수_있다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());
        jobRepository.incrementAttemptForRetry(job.getId(), 0, Instant.now());

        int updated = jobRepository.startProcessingIfAttemptMatches(job.getId(), 1, Instant.now());

        assertThat(updated).isEqualTo(1);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.PROCESSING);
    }

    @Test
    void 완료전이는_resultUrl을_함께_기록한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, Instant.now());

        int updated = jobRepository.completeIfAttemptMatches(
                job.getId(), "https://stub/image/1.png", 0, Instant.now());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(found.getResultUrl()).isEqualTo("https://stub/image/1.png");
    }

    @Test
    void 환불된_작업은_같은_attemptNo로_완료할_수_없다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.FAILED, 0, Instant.now());

        int updated = jobRepository.completeIfAttemptMatches(
                job.getId(), "https://stub/image/late.png", 0, Instant.now());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(updated).isZero();
        assertThat(found.getStatus()).isEqualTo(JobStatus.REFUNDED);
        assertThat(found.getResultUrl()).isNull();
    }

    @Test
    void 종결된_작업은_같은_attemptNo로_처리를_다시_시작할_수_없다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, Instant.now());
        jobRepository.completeIfAttemptMatches(
                job.getId(), "https://stub/image/done.png", 0, Instant.now());

        int updated = jobRepository.startProcessingIfAttemptMatches(job.getId(), 0, Instant.now());

        assertThat(updated).isZero();
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.COMPLETED);
    }

    @Test
    void 상태와_attemptNo가_모두_일치할_때만_전이된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());

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
        assertThat(beforeFail).isZero();

        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.FAILED, JobStatus.HOLDING, 0, Instant.now());
        int afterFail = jobRepository.incrementAttemptForRetry(job.getId(), 0, Instant.now());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(afterFail).isEqualTo(1);
        assertThat(found.getStatus()).isEqualTo(JobStatus.HOLDING);
        assertThat(found.getAttemptNo()).isEqualTo(1);
    }

    @Test
    void updatedAt이_cutoff_이전인_HOLDING_job만_조회된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        Instant staleUpdatedAt = Instant.now().minusSeconds(120);
        jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.HOLDING, JobStatus.HOLDING, 0, staleUpdatedAt);

        List<Job> caught = jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(
                JobStatus.HOLDING, Instant.now());
        List<Job> notCaught = jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(
                JobStatus.HOLDING, Instant.now().minusSeconds(300));

        assertThat(caught).extracting(Job::getId).containsExactly(job.getId());
        assertThat(notCaught).isEmpty();
    }
}
