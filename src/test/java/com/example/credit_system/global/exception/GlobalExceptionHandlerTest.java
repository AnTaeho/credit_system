package com.example.credit_system.global.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void 잔액부족_예외는_409와_코드를_반환한다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleInsufficientBalance(new InsufficientBalanceException(50, 100));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("INSUFFICIENT_BALANCE");
    }

    @Test
    void 잔액충돌_예외는_409와_코드를_반환한다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleBalanceConflict(new BalanceConflictException("conflict"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("BALANCE_CONFLICT");
    }

    @Test
    void 중복처리중_예외는_409와_코드를_반환한다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleDuplicateInProgress(new DuplicateRequestInProgressException());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("DUPLICATE_IN_PROGRESS");
    }
}
