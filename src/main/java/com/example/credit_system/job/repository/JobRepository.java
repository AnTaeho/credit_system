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

    // 이 리포지토리의 조건부 UPDATE 메서드들은 각각 그 자체로 원자적 연산 계약이므로, 호출부가
    // @Transactional인지에 기대지 않고 메서드 자체에 트랜잭션을 선언한다. @KafkaListener/@Scheduled처럼
    // 트랜잭션이 없는 컨텍스트에서 직접 호출해도 항상 안전하게 동작해야 하기 때문이다
    // (실제로 GenerationWorker.consume()에서 트랜잭션 없이 호출했다가 TransactionRequiredException을 겪었다).

    @Transactional
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

    @Transactional
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

    List<Job> findByStatusOrderByIdAsc(JobStatus status);

    List<Job> findByOrganizationIdOrderByIdDesc(Long organizationId);

    List<Job> findByStatusAndUpdatedAtBeforeOrderByIdAsc(JobStatus status, Instant cutoff);
}
