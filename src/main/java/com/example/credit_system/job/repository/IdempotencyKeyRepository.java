package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.IdempotencyKey;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, Long> {

    /** 조직과 멱등 키로 요청 기록을 조회한다. */
    Optional<IdempotencyKey> findByOrganizationIdAndIdemKey(Long organizationId, String idemKey);

    /** 멱등 키에 생성된 작업 ID를 연결한다. */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE IdempotencyKey k SET k.jobId = :jobId WHERE k.organizationId = :organizationId AND k.idemKey = :idemKey")
    int attachJobId(@Param("organizationId") Long organizationId,
                    @Param("idemKey") String idemKey,
                    @Param("jobId") Long jobId);
}
