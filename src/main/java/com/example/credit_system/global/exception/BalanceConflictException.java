package com.example.credit_system.global.exception;

public class BalanceConflictException extends RuntimeException {

    public BalanceConflictException(String message) {
        super(message);
    }
}
