package com.example.credit_system.benchmark;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

public class OptimisticLockStrategy implements DeductStrategy {

    private static final int MAX_ATTEMPTS = 50;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate requiresNewTransactionTemplate;

    public OptimisticLockStrategy(JdbcTemplate jdbcTemplate, TransactionTemplate requiresNewTransactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.requiresNewTransactionTemplate = requiresNewTransactionTemplate;
    }

    @Override
    public String name() {
        return "optimistic-lock";
    }

    @Override
    public DeductOutcome deduct(long accountId, long amount) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Boolean outcome = attemptOnce(accountId, amount);
            if (outcome == null) {
                return new DeductOutcome(false, attempt);
            }
            if (outcome) {
                return new DeductOutcome(true, attempt);
            }
        }
        return new DeductOutcome(false, MAX_ATTEMPTS - 1);
    }

    private Boolean attemptOnce(long accountId, long amount) {
        return requiresNewTransactionTemplate.execute(status -> {
            AccountSnapshot snapshot = jdbcTemplate.queryForObject(
                    "SELECT balance, version FROM bench_account WHERE id = ?",
                    ACCOUNT_SNAPSHOT_ROW_MAPPER, accountId);
            if (snapshot == null || snapshot.balance() < amount) {
                return null;
            }
            int updated = jdbcTemplate.update(
                    "UPDATE bench_account SET balance = balance - ?, version = version + 1 WHERE id = ? AND version = ?",
                    amount, accountId, snapshot.version());
            return updated == 1;
        });
    }

    private static final RowMapper<AccountSnapshot> ACCOUNT_SNAPSHOT_ROW_MAPPER =
            (rs, rowNum) -> new AccountSnapshot(rs.getLong("balance"), rs.getLong("version"));

    private record AccountSnapshot(long balance, long version) {
    }
}
