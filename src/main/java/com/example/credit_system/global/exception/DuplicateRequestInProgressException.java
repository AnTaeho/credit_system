package com.example.credit_system.global.exception;

public class DuplicateRequestInProgressException extends RuntimeException {

    /** 처리 중인 중복 요청 예외를 생성한다. */
    public DuplicateRequestInProgressException() {
        super("동일한 요청이 처리 중입니다. 잠시 후 다시 시도해주세요.");
    }
}
