package com.example.credit_system.job.worker;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GenerationWorkerUnitTest {

    @Mock JobRepository jobRepository;
    @Mock GenerationJobProcessor jobProcessor;

    GenerationWorker worker;
    Job job;

    @BeforeEach
    void setUp() {
        worker = new GenerationWorker(jobRepository, jobProcessor, 20);
        job = Job.hold(10L, 100L, "cat");
        ReflectionTestUtils.setField(job, "id", 1L);
    }

    @Test
    void 대기_작업을_DB에서_찾아_선점한_뒤_처리기로_넘긴다() {
        when(jobRepository.findByStatusOrderByIdAsc(eq(com.example.credit_system.job.domain.JobStatus.HOLDING), any()))
                .thenReturn(List.of(job));
        when(jobRepository.startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class))).thenReturn(1);

        worker.processPendingJobs();

        verify(jobProcessor).process(job);
    }

    @Test
    void 다른_워커가_선점한_작업은_외부_처리기로_넘기지_않는다() {
        when(jobRepository.startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class))).thenReturn(0);

        worker.claimAndProcess(job);

        verify(jobProcessor, never()).process(job);
    }
}
