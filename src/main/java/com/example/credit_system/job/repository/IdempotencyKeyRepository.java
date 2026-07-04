package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.IdempotencyKey;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, Long> {

    Optional<IdempotencyKey> findByOrganizationIdAndIdemKey(Long organizationId, String idemKey);

    @Modifying(clearAutomatically = true)
    @Query("UPDATE IdempotencyKey k SET k.jobId = :jobId WHERE k.organizationId = :organizationId AND k.idemKey = :idemKey")
    int attachJobId(@Param("organizationId") Long organizationId,
                    @Param("idemKey") String idemKey,
                    @Param("jobId") Long jobId);
}
