package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.JobLifecycleService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
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
        when(heartbeatRegistry.findExpiredAttempts()).thenReturn(Set.of());
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
        when(jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(
                eq(JobStatus.PROCESSING), any(Instant.class), any(Pageable.class)))
                .thenReturn(List.of(job));
        when(heartbeatRegistry.hasLiveHeartbeat(20L, 0)).thenReturn(false);
        when(jobRepository.transitionIfStatusAndAttemptMatch(
                eq(20L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), anyInt(), any(Instant.class)))
                .thenReturn(1);

        task.scan();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(20L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(0), any(Instant.class));
        verify(heartbeatRegistry).removeHeartbeat(20L, 0);
    }

    @Test
    void 정체된_PROCESSING_job이라도_live_heartbeat가_있으면_회수하지_않는다() {
        Job job = staleProcessingJob(21L);
        when(jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(
                eq(JobStatus.PROCESSING), any(Instant.class), any(Pageable.class)))
                .thenReturn(List.of(job));
        when(heartbeatRegistry.hasLiveHeartbeat(21L, 0)).thenReturn(true);

        task.scan();

        verify(jobRepository, never()).transitionIfStatusAndAttemptMatch(
                eq(21L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), anyInt(), any(Instant.class));
    }

    @Test
    void 만료_회수는_findById_재조회_없이_heartbeat가_알려준_attemptNo로_전이한다() {
        when(heartbeatRegistry.findExpiredAttempts()).thenReturn(Set.of(new JobAttempt(31L, 3)));
        when(jobRepository.transitionIfStatusAndAttemptMatch(
                eq(31L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(3), any(Instant.class)))
                .thenReturn(1);

        task.scan();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(31L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(3), any(Instant.class));
        verify(heartbeatRegistry).removeHeartbeat(31L, 3);
        verify(jobRepository, never()).findById(any());
    }

    @Test
    void 실패한_작업이_최대_attempt_미만이면_재시도한다() {
        Job job = failedJob(40L, 1);
        when(jobRepository.findByStatusOrderByIdAsc(eq(JobStatus.FAILED), any(Pageable.class))).thenReturn(List.of(job));

        task.scan();

        verify(jobLifecycleService).retry(job);
        verify(jobLifecycleService, never()).finalRefund(any(Job.class));
    }

    @Test
    void 실패한_작업이_최대_attempt에_도달하면_환불한다() {
        Job job = failedJob(41L, 2);
        when(jobRepository.findByStatusOrderByIdAsc(eq(JobStatus.FAILED), any(Pageable.class))).thenReturn(List.of(job));

        task.scan();

        verify(jobLifecycleService).finalRefund(job);
        verify(jobLifecycleService, never()).retry(any(Job.class));
    }

    @Test
    void FAILED_스냅샷을_그대로_넘기고_최신_상태_판정은_조건부_UPDATE에_맡긴다() {
        Job staleSnapshot = failedJob(42L, 2);
        when(jobRepository.findByStatusOrderByIdAsc(eq(JobStatus.FAILED), any(Pageable.class)))
                .thenReturn(List.of(staleSnapshot));

        task.scan();

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobLifecycleService).finalRefund(jobCaptor.capture());
        assertThat(jobCaptor.getValue()).isSameAs(staleSnapshot);
        verify(jobLifecycleService, never()).retry(any(Job.class));
    }

    @Test
    void 한_job의_환불_실패가_같은_주기의_나머지_job을_막지_않는다() {
        Job job1 = failedJob(1L, 2);
        Job job2 = failedJob(2L, 2);
        when(jobRepository.findByStatusOrderByIdAsc(eq(JobStatus.FAILED), any(Pageable.class)))
                .thenReturn(List.of(job1, job2));
        doThrow(new IllegalStateException("조직 행 없음")).when(jobLifecycleService).finalRefund(job1);

        task.scan();

        verify(jobLifecycleService).finalRefund(job2);
    }

    @Test
    void heartbeat_만료_회수_실패가_나머지_만료_job을_막지_않는다() {
        Set<JobAttempt> expiredAttempts = new LinkedHashSet<>(List.of(new JobAttempt(1L, 0), new JobAttempt(2L, 0)));
        when(heartbeatRegistry.findExpiredAttempts()).thenReturn(expiredAttempts);
        when(jobRepository.transitionIfStatusAndAttemptMatch(
                eq(1L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(0), any(Instant.class)))
                .thenThrow(new RuntimeException("DB 오류"));
        when(jobRepository.transitionIfStatusAndAttemptMatch(
                eq(2L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(0), any(Instant.class)))
                .thenReturn(1);

        task.scan();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(2L), eq(JobStatus.FAILED), eq(JobStatus.PROCESSING), eq(0), any(Instant.class));
        verify(heartbeatRegistry).removeHeartbeat(2L, 0);
        verify(heartbeatRegistry, never()).removeHeartbeat(1L, 0);
    }

    @Test
    void 한_단계의_실패가_다음_단계를_막지_않는다() {
        doThrow(new RuntimeException("PROCESSING 조회 실패"))
                .when(jobRepository).findByStatusAndUpdatedAtBeforeOrderByIdAsc(
                        eq(JobStatus.PROCESSING), any(Instant.class), any(Pageable.class));
        Job job = failedJob(50L, 1);
        when(jobRepository.findByStatusOrderByIdAsc(eq(JobStatus.FAILED), any(Pageable.class))).thenReturn(List.of(job));

        task.scan();

        verify(jobLifecycleService).retry(job);
    }

    @Test
    void FAILED_job은_배치_크기만큼만_조회한다() {
        task.scan();

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository).findByStatusOrderByIdAsc(eq(JobStatus.FAILED), pageableCaptor.capture());
        assertThat(pageableCaptor.getValue().getPageSize()).isEqualTo(100);
    }

    @Test
    void PROCESSING_정체_조회도_배치_크기만큼만_가져온다() {
        task.scan();

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository).findByStatusAndUpdatedAtBeforeOrderByIdAsc(
                eq(JobStatus.PROCESSING), any(Instant.class), pageableCaptor.capture());
        assertThat(pageableCaptor.getValue().getPageSize()).isEqualTo(100);
    }
}
