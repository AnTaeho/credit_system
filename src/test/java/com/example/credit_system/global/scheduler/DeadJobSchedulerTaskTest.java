package com.example.credit_system.global.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.RefundService;
import com.example.credit_system.job.service.RetryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
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
    RetryService retryService;

    @Mock
    RefundService refundService;

    DeadJobSchedulerTask task;

    @BeforeEach
    void setUp() {
        AppProperties appProperties = new AppProperties(
                new AppProperties.Generation(100L, 3), null, null, null,
                new AppProperties.Holding(60), new AppProperties.Processing(60));
        task = new DeadJobSchedulerTask(heartbeatRegistry, jobRepository, retryService, refundService, appProperties);
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

    @Test
    void 정체된_HOLDING_job은_FAILED로_전이를_시도한다() {
        Job job = staleHoldingJob(10L);
        when(jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(eq(JobStatus.HOLDING), any(Instant.class)))
                .thenReturn(List.of(job));
        when(jobRepository.transitionIfStatusAndAttemptMatch(
                eq(10L), eq(JobStatus.FAILED), eq(JobStatus.HOLDING), anyInt(), any(Instant.class)))
                .thenReturn(1);

        task.scan();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(10L), eq(JobStatus.FAILED), eq(JobStatus.HOLDING), eq(0), any(Instant.class));
    }

    @Test
    void 전이가_경합으로_0행이어도_예외없이_진행한다() {
        Job job = staleHoldingJob(11L);
        when(jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(eq(JobStatus.HOLDING), any(Instant.class)))
                .thenReturn(List.of(job));
        when(jobRepository.transitionIfStatusAndAttemptMatch(
                eq(11L), eq(JobStatus.FAILED), eq(JobStatus.HOLDING), anyInt(), any(Instant.class)))
                .thenReturn(0);

        task.scan();

        verify(jobRepository).transitionIfStatusAndAttemptMatch(
                eq(11L), eq(JobStatus.FAILED), eq(JobStatus.HOLDING), eq(0), any(Instant.class));
    }

    @Test
    void 정체된_PROCESSING_job은_heartbeat가_없으면_FAILED로_전이한다() {
        Job job = staleProcessingJob(20L);
        when(jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(eq(JobStatus.HOLDING), any(Instant.class)))
                .thenReturn(List.of());
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
        when(jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(eq(JobStatus.HOLDING), any(Instant.class)))
                .thenReturn(List.of());
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
}
