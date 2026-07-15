package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

public interface JobRepository extends JpaRepository<Job, Long> {

    /** 유효한 대기·처리 작업을 처리 상태로 선점한다. */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.status = com.example.credit_system.job.domain.JobStatus.PROCESSING,
                j.updatedAt = :now
            WHERE j.id = :jobId
              AND j.status IN (
                  com.example.credit_system.job.domain.JobStatus.HOLDING,
                  com.example.credit_system.job.domain.JobStatus.PROCESSING
              )
              AND j.attemptNo = :attemptNo
            """)
    int startProcessingIfAttemptMatches(@Param("jobId") Long jobId,
                                        @Param("attemptNo") int attemptNo,
                                        @Param("now") Instant now);

    /** 시도 번호가 일치하는 작업을 완료 처리한다. */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.status = com.example.credit_system.job.domain.JobStatus.COMPLETED,
                j.resultUrl = :resultUrl, j.updatedAt = :now
            WHERE j.id = :jobId
              AND j.status = com.example.credit_system.job.domain.JobStatus.PROCESSING
              AND j.attemptNo = :attemptNo
            """)
    int completeIfAttemptMatches(@Param("jobId") Long jobId,
                                 @Param("resultUrl") String resultUrl,
                                 @Param("attemptNo") int attemptNo,
                                 @Param("now") Instant now);

    /** 상태와 시도 번호가 모두 일치할 때 상태를 변경한다. */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.status = :newStatus, j.updatedAt = :now
            WHERE j.id = :jobId AND j.status = :expectedStatus AND j.attemptNo = :attemptNo
            """)
    int transitionIfStatusAndAttemptMatch(@Param("jobId") Long jobId,
                                          @Param("newStatus") JobStatus newStatus,
                                          @Param("expectedStatus") JobStatus expectedStatus,
                                          @Param("attemptNo") int attemptNo,
                                          @Param("now") Instant now);

    /** 실패 작업의 시도 번호를 높이고 처리 상태로 변경한다. */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.attemptNo = j.attemptNo + 1,
                j.status = com.example.credit_system.job.domain.JobStatus.PROCESSING,
                j.updatedAt = :now
            WHERE j.id = :jobId
              AND j.status = com.example.credit_system.job.domain.JobStatus.FAILED
              AND j.attemptNo = :expectedAttemptNo
            """)
    int incrementAttemptForRetry(@Param("jobId") Long jobId,
                                 @Param("expectedAttemptNo") int expectedAttemptNo,
                                 @Param("now") Instant now);

    /** 상태별 작업을 ID 오름차순으로 조회한다. */
    List<Job> findByStatusOrderByIdAsc(JobStatus status);

    /** 조직별 작업을 최신순으로 조회한다. */
    List<Job> findByOrganizationIdOrderByIdDesc(Long organizationId);

    /** 기준 시각 이전에 갱신된 상태별 작업을 조회한다. */
    List<Job> findByStatusAndUpdatedAtBeforeOrderByIdAsc(JobStatus status, Instant cutoff);
}
