package com.example.credit_system.ledger.dto;

import com.example.credit_system.ledger.domain.LedgerEntry;

import java.time.Instant;

public record LedgerResponse(Long id, String type, long amount, Long jobId, Instant createdAt) {

    /** 원장 엔티티를 응답 객체로 변환한다. */
    public static LedgerResponse from(LedgerEntry entry) {
        return new LedgerResponse(entry.getId(), entry.getType().name(), entry.getAmount(),
                entry.getJobId(), entry.getCreatedAt());
    }
}
