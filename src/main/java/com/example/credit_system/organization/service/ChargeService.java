package com.example.credit_system.organization.service;

import com.example.credit_system.global.exception.BalanceConflictException;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
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

    private static final int MAX_LOCK_RETRIES = 3;

    private final OrganizationRepository organizationRepository;
    private final LedgerRepository ledgerRepository;

    @Transactional
    public void charge(Long organizationId, long amount) {
        for (int attempt = 0; attempt < MAX_LOCK_RETRIES; attempt++) {
            Organization organization = organizationRepository.findById(organizationId)
                    .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 organization: " + organizationId));

            int updated = organizationRepository.addBalance(
                    organizationId, amount, organization.getVersion(), Instant.now());
            if (updated == 1) {
                ledgerRepository.save(LedgerEntry.of(organizationId, null, LedgerType.CHARGE, amount));
                log.info("충전 완료: organizationId={}, amount={}", organizationId, amount);
                return;
            }
            log.info("충전 중 버전 충돌, 재시도: organizationId={}, attempt={}", organizationId, attempt);
        }
        throw new BalanceConflictException("충전 처리 중 버전 충돌이 반복되었습니다.");
    }
}
