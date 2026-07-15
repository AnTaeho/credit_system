package com.example.credit_system.global.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /** 잔액 부족을 충돌 응답으로 변환한다. */
    @ExceptionHandler(InsufficientBalanceException.class)
    public ResponseEntity<ErrorResponse> handleInsufficientBalance(InsufficientBalanceException e) {
        return conflict("INSUFFICIENT_BALANCE", e.getMessage());
    }

    /** 처리 중인 중복 요청을 충돌 응답으로 변환한다. */
    @ExceptionHandler(DuplicateRequestInProgressException.class)
    public ResponseEntity<ErrorResponse> handleDuplicateInProgress(DuplicateRequestInProgressException e) {
        return conflict("DUPLICATE_IN_PROGRESS", e.getMessage());
    }

    /** 잘못된 요청을 400 응답으로 변환한다. */
    @ExceptionHandler(InvalidRequestException.class)
    public ResponseEntity<ErrorResponse> handleInvalidRequest(InvalidRequestException e) {
        return ResponseEntity.badRequest().body(new ErrorResponse("INVALID_REQUEST", e.getMessage()));
    }

    /** 동시 중복 삽입 충돌을 중복 요청 응답으로 변환한다. */
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ErrorResponse> handleDataIntegrityViolation(DataIntegrityViolationException e) {
        return conflict("DUPLICATE_IN_PROGRESS", "동일한 요청이 동시에 처리 중입니다. 잠시 후 다시 시도해주세요.");
    }

    /** 비즈니스 충돌 응답을 생성한다. */
    private ResponseEntity<ErrorResponse> conflict(String code, String message) {
        log.info("business exception: code={}, message={}", code, message);
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(code, message));
    }
}
