package com.example.credit_system.global.exception;

public class InsufficientBalanceException extends RuntimeException {

    public InsufficientBalanceException(long balance, long required) {
        super("잔액이 부족합니다. balance=" + balance + ", required=" + required);
    }
}
