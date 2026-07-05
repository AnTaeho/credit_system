package com.example.credit_system.organization.repository;

import com.example.credit_system.organization.domain.Organization;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface OrganizationRepository extends JpaRepository<Organization, Long> {

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Organization o
            SET o.balance = o.balance - :amount, o.version = o.version + 1, o.updatedAt = :now
            WHERE o.id = :id AND o.balance >= :amount
            """)
    int deductBalance(@Param("id") Long id,
                      @Param("amount") long amount,
                      @Param("now") Instant now);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Organization o
            SET o.balance = o.balance + :amount, o.version = o.version + 1, o.updatedAt = :now
            WHERE o.id = :id
            """)
    int addBalance(@Param("id") Long id,
                   @Param("amount") long amount,
                   @Param("now") Instant now);
}
