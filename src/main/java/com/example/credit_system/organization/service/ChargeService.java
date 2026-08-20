package com.example.credit_system.organization.service;

import com.example.credit_system.global.exception.InvalidRequestException;
import com.example.credit_system.global.exception.OrganizationNotFoundException;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.dto.ChargeResponse;
import com.example.credit_system.organization.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChargeService {

    private static final long MAX_CHARGE_AMOUNT = 1_000_000L;

    private final OrganizationRepository organizationRepository;
    private final LedgerRepository ledgerRepository;

    @Transactional
    public ChargeResponse charge(Long organizationId, String idemKey, long amount) {
        validateRequest(idemKey, amount);

        Optional<LedgerEntry> existing = ledgerRepository.findByOrganizationIdAndIdemKey(organizationId, idemKey);
        if (existing.isPresent()) {
            long balance = organizationRepository.findById(organizationId)
                    .orElseThrow(() -> new OrganizationNotFoundException(organizationId))
                    .getBalance();
            log.info("중복 충전 요청 감지: organizationId={}, idemKey={}", organizationId, idemKey);
            return new ChargeResponse(balance, true);
        }

        int updated = organizationRepository.addBalance(organizationId, amount, Instant.now());
        if (updated != 1) {
            throw new OrganizationNotFoundException(organizationId);
        }

        ledgerRepository.save(LedgerEntry.charge(organizationId, idemKey, amount));
        log.info("충전 완료: organizationId={}, amount={}", organizationId, amount);

        long balance = organizationRepository.findById(organizationId).orElseThrow().getBalance();
        return new ChargeResponse(balance, false);
    }

    private void validateRequest(String idemKey, long amount) {
        if (idemKey == null || idemKey.isBlank()) {
            throw new InvalidRequestException("idemKey는 필수입니다.");
        }
        if (idemKey.length() > 100) {
            throw new InvalidRequestException("idemKey는 100자를 초과할 수 없습니다.");
        }
        if (amount <= 0) {
            throw new InvalidRequestException("amount는 0보다 커야 합니다.");
        }
        if (amount > MAX_CHARGE_AMOUNT) {
            throw new InvalidRequestException("amount는 1,000,000을 초과할 수 없습니다.");
        }
    }
}
