package com.example.credit_system.global.exception;

public class DuplicateRequestInProgressException extends RuntimeException {

    public DuplicateRequestInProgressException() {
        super("동일한 요청이 처리 중입니다. 잠시 후 다시 시도해주세요.");
    }
}
