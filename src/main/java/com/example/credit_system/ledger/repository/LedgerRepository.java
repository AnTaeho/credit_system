package com.example.credit_system.ledger.repository;

import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.dto.LedgerBalanceCheck;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface LedgerRepository extends JpaRepository<LedgerEntry, Long> {

    List<LedgerEntry> findByOrganizationIdOrderByIdDesc(Long organizationId);

    @Query("""
            SELECT new com.example.credit_system.ledger.dto.LedgerBalanceCheck(
                o.id, o.balance, o.initialBalance, COALESCE(SUM(l.amount), 0L))
            FROM Organization o
            LEFT JOIN LedgerEntry l ON l.organizationId = o.id
            WHERE o.id > :lastId
            GROUP BY o.id, o.balance, o.initialBalance
            ORDER BY o.id
            """)
    List<LedgerBalanceCheck> findBalanceChecksAfter(@Param("lastId") Long lastId, Pageable pageable);
}
