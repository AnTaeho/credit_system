package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface JobRepository extends JpaRepository<Job, Long> {

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.status = :status, j.updatedAt = :now
            WHERE j.id = :jobId AND j.attemptNo = :attemptNo
            """)
    int updateStatusIfAttemptMatches(@Param("jobId") Long jobId,
                                     @Param("status") JobStatus status,
                                     @Param("attemptNo") int attemptNo,
                                     @Param("now") Instant now);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.status = com.example.credit_system.job.domain.JobStatus.COMPLETED,
                j.resultUrl = :resultUrl, j.updatedAt = :now
            WHERE j.id = :jobId AND j.attemptNo = :attemptNo
            """)
    int completeIfAttemptMatches(@Param("jobId") Long jobId,
                                 @Param("resultUrl") String resultUrl,
                                 @Param("attemptNo") int attemptNo,
                                 @Param("now") Instant now);

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

    List<Job> findByStatusOrderByIdAsc(JobStatus status);

    List<Job> findByOrganizationIdOrderByIdDesc(Long organizationId);
}
