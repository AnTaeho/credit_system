package com.example.credit_system.benchmark;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class ConditionalUpdateStrategy implements DeductStrategy {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public ConditionalUpdateStrategy(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public String name() {
        return "conditional-update";
    }

    @Override
    public DeductOutcome deduct(long accountId, long amount) {
        Boolean success = transactionTemplate.execute(status -> {
            int updated = jdbcTemplate.update(
                    "UPDATE bench_account SET balance = balance - ? WHERE id = ? AND balance >= ?",
                    amount, accountId, amount);
            return updated == 1;
        });
        return new DeductOutcome(Boolean.TRUE.equals(success), 0);
    }
}
