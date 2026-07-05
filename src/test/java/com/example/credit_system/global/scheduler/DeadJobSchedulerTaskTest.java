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
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
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
                new AppProperties.Holding(60));
        task = new DeadJobSchedulerTask(heartbeatRegistry, jobRepository, retryService, refundService, appProperties);
        when(heartbeatRegistry.findExpiredJobIds()).thenReturn(Set.of());
        when(jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)).thenReturn(List.of());
    }

    private static Job staleHoldingJob(long id) {
        Job job = Job.hold(1L, 100L, "cat");
        ReflectionTestUtils.setField(job, "id", id);
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
}
