package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.JobLifecycleService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeadJobSchedulerTaskTest {

    @Mock
    HeartbeatRegistry heartbeatRegistry;

    @Mock
    JobRepository jobRepository;

    @Mock
    JobLifecycleService jobLifecycleService;

    DeadJobSchedulerTask task;

    @BeforeEach
    void setUp() {
        AppProperties appProperties = new AppProperties(
                new AppProperties.Generation(100L, 3), null, null, new AppProperties.Processing(60));
        task = new DeadJobSchedulerTask(heartbeatRegistry, jobRepository, jobLifecycleService, appProperties);
        when(heartbeatRegistry.findExpiredJobIds()).thenReturn(Set.of());
        when(jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)).thenReturn(List.of());
    }

    private static Job staleHoldingJob(long id) {
        Job job = Job.hold(1L, 100L, "cat");
        ReflectionTestUtils.setField(job, "id", id);
        return job;
    }

    private static Job staleProcessingJob(long id) {
        Job job = Job.hold(1L, 100L, "cat");
        ReflectionTestUtils.setField(job, "id", id);
        ReflectionTestUtils.setField(job, "status", JobStatus.PROCESSING);
        return job;
    }

    private static Job failedJob(long id, int attemptNo) {
        Job job = Job.hold(1L, 100L, "cat");
        ReflectionTestUtils.setField(job, "id", id);
        ReflectionTestUtils.setField(job, "status", JobStatus.FAILED);
        ReflectionTestUtils.setField(job, "attemptNo", attemptNo);
        return job;
    }


    @Test
    void 정체된_PROCESSING_job은_heartbeat가_없으면_FAILED로_전이한다() {
        Job job = staleProcessingJob(20L);
        when(jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(eq(JobStatus.PROCESSING), any(Instant.class)))
                .thenReturn(List.of(job));
        when(heartbeatRegistry.hasLiveHeartbeat(20L)).thenReturn(false);
        when(jobRepository.transitionIfStatusAndAttemptMatch(
                eq(20L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), anyInt(), any(Instant.class)))
                .thenReturn(1);

        task.scan();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(20L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(0), any(Instant.class));
        verify(heartbeatRegistry).remove(20L);
    }

    @Test
    void 정체된_PROCESSING_job이라도_live_heartbeat가_있으면_회수하지_않는다() {
        Job job = staleProcessingJob(21L);
        when(jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(eq(JobStatus.PROCESSING), any(Instant.class)))
                .thenReturn(List.of(job));
        when(heartbeatRegistry.hasLiveHeartbeat(21L)).thenReturn(true);

        task.scan();

        verify(jobRepository, never()).transitionIfStatusAndAttemptMatch(
                eq(21L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), anyInt(), any(Instant.class));
    }

    @Test
    void 만료된_heartbeat는_PROCESSING_작업만_FAILED로_전이한다() {
        Job job = staleProcessingJob(30L);
        when(heartbeatRegistry.findExpiredJobIds()).thenReturn(Set.of(30L));
        when(jobRepository.findById(30L)).thenReturn(Optional.of(job));
        when(jobRepository.transitionIfStatusAndAttemptMatch(
                eq(30L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(0), any(Instant.class)))
                .thenReturn(1);

        task.scan();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(30L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(0), any(Instant.class));
        verify(heartbeatRegistry).remove(30L);
    }

    @Test
    void 실패한_작업이_최대_attempt_미만이면_재시도한다() {
        Job snapshot = failedJob(40L, 1);
        Job current = failedJob(40L, 1);
        when(jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)).thenReturn(List.of(snapshot));
        when(jobRepository.findById(40L)).thenReturn(Optional.of(current));

        task.scan();

        verify(jobLifecycleService).retry(current);
        verify(jobLifecycleService, never()).finalRefund(any(Job.class));
    }

    @Test
    void 실패한_작업이_최대_attempt에_도달하면_환불한다() {
        Job snapshot = failedJob(41L, 2);
        Job current = failedJob(41L, 2);
        when(jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)).thenReturn(List.of(snapshot));
        when(jobRepository.findById(41L)).thenReturn(Optional.of(current));

        task.scan();

        verify(jobLifecycleService).finalRefund(current);
        verify(jobLifecycleService, never()).retry(any(Job.class));
    }

    @Test
    void FAILED_스냅샷이더라도_재조회한_상태가_종결이면_처리하지_않는다() {
        Job snapshot = failedJob(42L, 2);
        Job completed = failedJob(42L, 2);
        ReflectionTestUtils.setField(completed, "status", JobStatus.COMPLETED);
        when(jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)).thenReturn(List.of(snapshot));
        when(jobRepository.findById(42L)).thenReturn(Optional.of(completed));

        task.scan();

        verify(jobLifecycleService, never()).retry(any(Job.class));
        verify(jobLifecycleService, never()).finalRefund(any(Job.class));
    }

    @Test
    void 한_job의_환불_실패가_같은_주기의_나머지_job을_막지_않는다() {
        Job snapshot1 = failedJob(1L, 2);
        Job snapshot2 = failedJob(2L, 2);
        Job current1 = failedJob(1L, 2);
        Job current2 = failedJob(2L, 2);
        when(jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)).thenReturn(List.of(snapshot1, snapshot2));
        when(jobRepository.findById(1L)).thenReturn(Optional.of(current1));
        when(jobRepository.findById(2L)).thenReturn(Optional.of(current2));
        doThrow(new IllegalStateException("조직 행 없음")).when(jobLifecycleService).finalRefund(current1);

        task.scan();

        verify(jobLifecycleService).finalRefund(current2);
    }

    @Test
    void heartbeat_만료_회수_실패가_나머지_만료_job을_막지_않는다() {
        Set<Long> expiredIds = new LinkedHashSet<>(List.of(1L, 2L));
        when(heartbeatRegistry.findExpiredJobIds()).thenReturn(expiredIds);
        when(jobRepository.findById(1L)).thenThrow(new RuntimeException("DB 오류"));
        Job job2 = staleHoldingJob(2L);
        when(jobRepository.findById(2L)).thenReturn(Optional.of(job2));

        task.scan();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(2L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), anyInt(), any(Instant.class));
    }

    @Test
    void 한_단계의_실패가_다음_단계를_막지_않는다() {
        doThrow(new RuntimeException("PROCESSING 조회 실패"))
                .when(jobRepository).findByStatusAndUpdatedAtBeforeOrderByIdAsc(eq(JobStatus.PROCESSING), any(Instant.class));
        Job snapshot = failedJob(50L, 1);
        Job current = failedJob(50L, 1);
        when(jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)).thenReturn(List.of(snapshot));
        when(jobRepository.findById(50L)).thenReturn(Optional.of(current));

        task.scan();

        verify(jobLifecycleService).retry(current);
    }
}
