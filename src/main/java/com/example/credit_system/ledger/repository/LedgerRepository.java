package com.example.credit_system.ledger.repository;

import com.example.credit_system.ledger.domain.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LedgerRepository extends JpaRepository<LedgerEntry, Long> {

    /** 조직별 원장 항목을 최신순으로 조회한다. */
    List<LedgerEntry> findByOrganizationIdOrderByIdDesc(Long organizationId);
}
