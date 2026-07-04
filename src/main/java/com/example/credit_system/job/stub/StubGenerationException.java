package com.example.credit_system.job.stub;

public class StubGenerationException extends RuntimeException {

    public StubGenerationException(String prompt) {
        super("이미지 생성 stub 실패: prompt=" + prompt);
    }
}
