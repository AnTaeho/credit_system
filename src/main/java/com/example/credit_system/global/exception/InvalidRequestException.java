package com.example.credit_system.global.exception;

public class InvalidRequestException extends RuntimeException {

    /** 잘못된 요청 값에 대한 예외를 생성한다. */
    public InvalidRequestException(String message) {
        super(message);
    }
}
