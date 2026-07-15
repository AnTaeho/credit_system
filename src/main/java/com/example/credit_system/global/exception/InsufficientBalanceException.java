package com.example.credit_system.global.exception;

public class InsufficientBalanceException extends RuntimeException {

    /** 현재 잔액과 필요 금액으로 잔액 부족 예외를 생성한다. */
    public InsufficientBalanceException(long balance, long required) {
        super("잔액이 부족합니다. balance=" + balance + ", required=" + required);
    }
}
