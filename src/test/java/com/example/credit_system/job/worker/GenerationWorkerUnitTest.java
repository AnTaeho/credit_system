package com.example.credit_system.job.worker;

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
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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

        worker.processPendingJobs();

        verify(jobProcessor).process(job);
    }

    @Test
    void 다른_워커가_선점한_작업은_외부_처리기로_넘기지_않는다() {
        when(jobRepository.startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class))).thenReturn(0);

        worker.claimAndDispatch(job);

        verify(jobProcessor, never()).process(job);
    }

    /**
     * 선점 UPDATE가 DB 장애로 던질 때 permit이 새면 concurrency회 누적만으로 워커가 영구 정지한다.
     * DB가 회복된 뒤 다시 dispatch되는지까지 확인해야 회귀를 잡을 수 있다.
     */
    @Test
    void 선점_UPDATE가_반복_실패해도_permit이_고갈되지_않는다() {
        doReturn(List.of(job)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doThrow(new QueryTimeoutException("db unavailable"))
                .when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));

        for (int i = 0; i < CONCURRENCY + 2; i++) {
            worker.processPendingJobs();
        }

        assertThat(availablePermits()).isEqualTo(CONCURRENCY);
        verify(jobProcessor, never()).process(job);

        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));

        worker.processPendingJobs();

        verify(jobProcessor).process(job);
    }

    /**
     * dispatch에 성공하면 permit 반납 책임은 executor 스레드로 넘어간다.
     * 호출 스레드가 한 번 더 반납하면 동시 실행 상한이 조용히 늘어난다.
     */
    @Test
    void dispatch에_성공하면_permit을_이중_반납하지_않는다() {
        doReturn(List.of(job)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));

        worker.processPendingJobs();

        verify(jobProcessor).process(job);
        assertThat(availablePermits()).isEqualTo(CONCURRENCY);
    }

    /** 롤백 UPDATE까지 실패해도 permit은 반납되어야 한다. 롤백 실패 작업은 timeout 회수 경로가 맡는다. */
    @Test
    void executor_위임과_롤백이_모두_실패해도_permit을_반납한다() {
        GenerationWorker rejectingWorker = new GenerationWorker(jobRepository, jobProcessor,
                task -> { throw new IllegalStateException("executor shutdown"); },
                new WorkerProperties(true, 20, CONCURRENCY));
        doReturn(List.of(job)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));
        doThrow(new QueryTimeoutException("db unavailable")).when(jobRepository)
                .transitionIfStatusAndAttemptMatch(eq(1L), eq(JobStatus.HOLDING), eq(JobStatus.PROCESSING),
                        eq(0), any(Instant.class));

        rejectingWorker.processPendingJobs();

        assertThat(availablePermits(rejectingWorker)).isEqualTo(CONCURRENCY);
        verify(jobProcessor, never()).process(job);
    }

    /** 한 작업의 선점 실패가 루프를 중단시키면 같은 배치의 남은 작업이 통째로 다음 주기로 밀린다. */
    @Test
    void 한_작업의_선점_실패가_같은_배치의_나머지_작업을_막지_않는다() {
        Job second = Job.hold(10L, 100L, "dog");
        ReflectionTestUtils.setField(second, "id", 2L);
        doReturn(List.of(job, second)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doThrow(new QueryTimeoutException("db unavailable"))
                .when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(2L), eq(0), any(Instant.class));

        worker.processPendingJobs();

        verify(jobProcessor).process(second);
        assertThat(availablePermits()).isEqualTo(CONCURRENCY);
    }

    /** dispatch가 성공하기 전까지 permit이 유지되어야 하므로, 처리 도중 관측한 permit 수로도 상한을 확인한다. */
    @Test
    void 처리_중에는_permit이_점유되어_있다() {
        doReturn(List.of(job)).when(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.HOLDING), any());
        doReturn(1).when(jobRepository).startProcessingIfAttemptMatches(eq(1L), eq(0), any(Instant.class));
        int[] observed = new int[1];
        doAnswer(invocation -> {
            observed[0] = availablePermits();
            return null;
        }).when(jobProcessor).process(job);

        worker.processPendingJobs();

        assertThat(observed[0]).isEqualTo(CONCURRENCY - 1);
        assertThat(availablePermits()).isEqualTo(CONCURRENCY);
    }

    private int availablePermits() {
        return availablePermits(worker);
    }

    private int availablePermits(GenerationWorker target) {
        Semaphore semaphore = (Semaphore) ReflectionTestUtils.getField(target, "availableWorkers");
        return semaphore.availablePermits();
    }
}
