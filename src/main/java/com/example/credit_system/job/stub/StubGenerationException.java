package com.example.credit_system.job.stub;

public class StubGenerationException extends RuntimeException {

    /** 실패한 프롬프트로 생성 예외를 만든다. */
    public StubGenerationException(String prompt) {
        super("이미지 생성 stub 실패: prompt=" + prompt);
    }
}
