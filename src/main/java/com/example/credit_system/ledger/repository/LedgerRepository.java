package com.example.credit_system.ledger.repository;

import com.example.credit_system.ledger.domain.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LedgerRepository extends JpaRepository<LedgerEntry, Long> {

    List<LedgerEntry> findByOrganizationIdOrderByIdDesc(Long organizationId);
}
