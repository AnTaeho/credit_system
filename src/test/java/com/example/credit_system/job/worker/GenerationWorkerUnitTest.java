package com.example.credit_system.job.worker;

import com.example.credit_system.global.config.WorkerProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GenerationWorkerUnitTest {

    private static final int CONCURRENCY = 3;

    @Mock JobRepository jobRepository;
    @Mock GenerationJobProcessor jobProcessor;

    GenerationWorker worker;
    Job job;

    @BeforeEach
    void setUp() {
        worker = new GenerationWorker(jobRepository, jobProcessor, new SyncTaskExecutor(),
                new WorkerProperties(true, 20, CONCURRENCY));
        job = Job.hold(10L, 100L, "cat");
        ReflectionTestUtils.setField(job, "id", 1L);
    }

    @Test
    void 대기_작업을_DB에서_찾아_선점한_뒤_처리기로_넘긴다() {
        when(jobRepository.findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any()))
                .thenReturn(List.of(job));
        when(jobRepository.startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class))).thenReturn(1);

        worker.dispatchPendingJobs();

        verify(jobProcessor).runGeneration(job);
    }

    @Test
    void 다른_워커가_선점한_작업은_외부_처리기로_넘기지_않는다() {
        doReturn(List.of(job)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doReturn(0).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));

        worker.dispatchPendingJobs();

        verify(jobProcessor, never()).runGeneration(job);
    }

    @Test
    void 선점_UPDATE가_반복_실패해도_이후_주기에서_다시_처리한다() {
        doReturn(List.of(job)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doThrow(new QueryTimeoutException("db unavailable"))
                .when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));

        for (int i = 0; i < CONCURRENCY + 2; i++) {
            worker.dispatchPendingJobs();
        }

        verify(jobProcessor, never()).runGeneration(job);

        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));

        worker.dispatchPendingJobs();

        verify(jobProcessor).runGeneration(job);
    }

    @Test
    void executor_위임과_롤백이_모두_실패해도_예외가_새어나가지_않는다() {
        GenerationWorker rejectingWorker = new GenerationWorker(jobRepository, jobProcessor,
                task -> { throw new IllegalStateException("executor shutdown"); },
                new WorkerProperties(true, 20, CONCURRENCY));
        doReturn(List.of(job)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));
        doThrow(new QueryTimeoutException("db unavailable")).when(jobRepository)
                .transitionIfStatusAndAttemptMatch(eq(1L), eq(JobStatus.HOLDING), eq(JobStatus.PROCESSING),
                        eq(0), any(Instant.class));

        assertThatCode(rejectingWorker::dispatchPendingJobs).doesNotThrowAnyException();

        verify(jobProcessor, never()).runGeneration(job);
    }

    @Test
    void 한_작업의_선점_실패가_같은_배치의_나머지_작업을_막지_않는다() {
        Job second = Job.hold(10L, 100L, "dog");
        ReflectionTestUtils.setField(second, "id", 2L);
        doReturn(List.of(job, second)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doThrow(new QueryTimeoutException("db unavailable"))
                .when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(2L), eq(0), any(Instant.class));

        worker.dispatchPendingJobs();

        verify(jobProcessor).runGeneration(second);
    }

    @Test
    void executor가_거부하면_선점을_롤백하고_이번_주기를_중단한다() {
        Job second = Job.hold(10L, 100L, "dog");
        ReflectionTestUtils.setField(second, "id", 2L);
        GenerationWorker rejectingWorker = new GenerationWorker(jobRepository, jobProcessor,
                task -> { throw new TaskRejectedException("pool exhausted"); },
                new WorkerProperties(true, 20, CONCURRENCY));
        doReturn(List.of(job, second)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));

        rejectingWorker.dispatchPendingJobs();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(1L), eq(JobStatus.HOLDING), eq(JobStatus.PROCESSING), eq(0), any(Instant.class));
        verify(jobRepository, never()).startProcessingIfAttemptMatches(eq(2L), anyInt(), any(Instant.class));
    }

    @Test
    void dispatch에_성공하면_같은_배치의_다음_작업도_처리한다() {
        Job second = Job.hold(10L, 100L, "dog");
        ReflectionTestUtils.setField(second, "id", 2L);
        doReturn(List.of(job, second)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(2L), eq(0), any(Instant.class));

        worker.dispatchPendingJobs();

        verify(jobProcessor).runGeneration(job);
        verify(jobProcessor).runGeneration(second);
    }
}
