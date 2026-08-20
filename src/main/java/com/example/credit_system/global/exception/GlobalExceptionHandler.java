package com.example.credit_system.global.exception;

import lombok.extern.slf4j.Slf4j;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
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

    @ExceptionHandler(DuplicateRequestInProgressException.class)
    public ResponseEntity<ErrorResponse> handleDuplicateInProgress(DuplicateRequestInProgressException e) {
        return conflict("DUPLICATE_IN_PROGRESS", e.getMessage());
    }

    @ExceptionHandler(InvalidRequestException.class)
    public ResponseEntity<ErrorResponse> handleInvalidRequest(InvalidRequestException e) {
        return ResponseEntity.badRequest().body(new ErrorResponse("INVALID_REQUEST", e.getMessage()));
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ErrorResponse> handleDataIntegrityViolation(DataIntegrityViolationException e) {
        if (isUniqueConstraintViolation(e)) {
            return conflict("DUPLICATE_IN_PROGRESS", "동일한 요청이 동시에 처리 중입니다. 잠시 후 다시 시도해주세요.");
        }
        log.error("무결성 제약 위반: {}", e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("DATA_INTEGRITY_VIOLATION", "요청을 처리할 수 없습니다."));
    }

    private boolean isUniqueConstraintViolation(Throwable e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof ConstraintViolationException cve) {
                return cve.getKind() == ConstraintViolationException.ConstraintKind.UNIQUE;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @ExceptionHandler(OrganizationNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleOrganizationNotFound(OrganizationNotFoundException e) {
        log.info("business exception: code=ORGANIZATION_NOT_FOUND, message={}", e.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("ORGANIZATION_NOT_FOUND", e.getMessage()));
    }

    private ResponseEntity<ErrorResponse> conflict(String code, String message) {
        log.info("business exception: code={}, message={}", code, message);
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(code, message));
    }
}
