package com.example.credit_system.organization.repository;

import com.example.credit_system.organization.domain.Organization;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface OrganizationRepository extends JpaRepository<Organization, Long> {

    /** 잔액이 충분한 조직에서 금액을 원자적으로 차감한다. */
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Organization o
            SET o.balance = o.balance - :amount, o.updatedAt = :now
            WHERE o.id = :id AND o.balance >= :amount
            """)
    int deductBalance(@Param("id") Long id,
                      @Param("amount") long amount,
                      @Param("now") Instant now);

    /** 조직 잔액에 금액을 원자적으로 더한다. */
    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Organization o
            SET o.balance = o.balance + :amount, o.updatedAt = :now
            WHERE o.id = :id
            """)
    int addBalance(@Param("id") Long id,
                   @Param("amount") long amount,
                   @Param("now") Instant now);
}
