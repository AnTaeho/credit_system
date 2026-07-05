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

    @ExceptionHandler(InsufficientBalanceException.class)
    public ResponseEntity<ErrorResponse> handleInsufficientBalance(InsufficientBalanceException e) {
        return conflict("INSUFFICIENT_BALANCE", e.getMessage());
    }

    @ExceptionHandler(DuplicateRequestInProgressException.class)
    public ResponseEntity<ErrorResponse> handleDuplicateInProgress(DuplicateRequestInProgressException e) {
        return conflict("DUPLICATE_IN_PROGRESS", e.getMessage());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ErrorResponse> handleDataIntegrityViolation(DataIntegrityViolationException e) {
        // 동시에 들어온 두 요청이 모두 중복 조회(SELECT)를 통과한 뒤 같은 idem_key로 INSERT를 시도하는
        // 극히 드문 경합 상황에서만 여기까지 도달한다. HoldService는 이 경우를 세션 안에서 잡지 않고
        // 트랜잭션 전체를 롤백시키므로, 여기서 같은 "중복 처리 중" 응답으로 번역한다.
        return conflict("DUPLICATE_IN_PROGRESS", "동일한 요청이 동시에 처리 중입니다. 잠시 후 다시 시도해주세요.");
    }

    private ResponseEntity<ErrorResponse> conflict(String code, String message) {
        log.info("business exception: code={}, message={}", code, message);
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(code, message));
    }
}
