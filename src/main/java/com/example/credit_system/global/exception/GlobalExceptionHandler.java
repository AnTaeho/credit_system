package com.example.credit_system.global.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InsufficientBalanceException.class)
    public ResponseEntity<ErrorResponse> handleInsufficientBalance(InsufficientBalanceException e) {
        return conflict("INSUFFICIENT_BALANCE", e.getMessage());
    }

    @ExceptionHandler(BalanceConflictException.class)
    public ResponseEntity<ErrorResponse> handleBalanceConflict(BalanceConflictException e) {
        return conflict("BALANCE_CONFLICT", e.getMessage());
    }

    @ExceptionHandler(DuplicateRequestInProgressException.class)
    public ResponseEntity<ErrorResponse> handleDuplicateInProgress(DuplicateRequestInProgressException e) {
        return conflict("DUPLICATE_IN_PROGRESS", e.getMessage());
    }

    private ResponseEntity<ErrorResponse> conflict(String code, String message) {
        log.info("business exception: code={}, message={}", code, message);
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(code, message));
    }
}
