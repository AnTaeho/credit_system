package com.example.credit_system.benchmark;

public interface DeductStrategy {

    String name();

    DeductOutcome deduct(long accountId, long amount);

    record DeductOutcome(boolean success, int retries) {
    }
}
