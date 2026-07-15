package com.example.credit_system.global.exception;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
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
    void 중복처리중_예외는_409와_코드를_반환한다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleDuplicateInProgress(new DuplicateRequestInProgressException());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("DUPLICATE_IN_PROGRESS");
    }

    @Test
    void 유니크_제약_위반은_중복_처리중으로_번역된다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleDataIntegrityViolation(new DataIntegrityViolationException("constraint violated"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("DUPLICATE_IN_PROGRESS");
    }

    @Test
    void 잘못된_요청은_400과_코드를_반환한다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleInvalidRequest(new InvalidRequestException("잘못된 요청"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().code()).isEqualTo("INVALID_REQUEST");
        assertThat(response.getBody().message()).isEqualTo("잘못된 요청");
    }
}
