package com.example.credit_system.benchmark;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class PessimisticLockStrategy implements DeductStrategy {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public PessimisticLockStrategy(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public String name() {
        return "pessimistic-lock";
    }

    @Override
    public DeductOutcome deduct(long accountId, long amount) {
        Boolean success = transactionTemplate.execute(status -> {
            Long balance = jdbcTemplate.queryForObject(
                    "SELECT balance FROM bench_account WHERE id = ? FOR UPDATE",
                    Long.class, accountId);
            if (balance == null || balance < amount) {
                return false;
            }
            int updated = jdbcTemplate.update(
                    "UPDATE bench_account SET balance = balance - ? WHERE id = ?",
                    amount, accountId);
            return updated == 1;
        });
        return new DeductOutcome(Boolean.TRUE.equals(success), 0);
    }
}
