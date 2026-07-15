package com.example.credit_system.organization.service;

import com.example.credit_system.global.exception.InvalidRequestException;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChargeService {

    private final OrganizationRepository organizationRepository;
    private final LedgerRepository ledgerRepository;

    /** 조직 잔액을 충전하고 원장에 기록한다. */
    @Transactional
    public void charge(Long organizationId, long amount) {
        if (amount <= 0) {
            throw new InvalidRequestException("amount는 0보다 커야 합니다.");
        }
        int updated = organizationRepository.addBalance(organizationId, amount, Instant.now());
        if (updated == 1) {
            ledgerRepository.save(LedgerEntry.of(organizationId, null, LedgerType.CHARGE, amount));
            log.info("충전 완료: organizationId={}, amount={}", organizationId, amount);
            return;
        }
        throw new IllegalArgumentException("존재하지 않는 organization: " + organizationId);
    }
}
