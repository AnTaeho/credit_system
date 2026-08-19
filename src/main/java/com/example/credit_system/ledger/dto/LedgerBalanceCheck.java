package com.example.credit_system.ledger.dto;

public record LedgerBalanceCheck(Long organizationId, long balance, long initialBalance, long ledgerSum) {
}
