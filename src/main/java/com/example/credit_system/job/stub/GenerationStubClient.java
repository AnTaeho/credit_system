package com.example.credit_system.job.stub;

import com.example.credit_system.global.config.AppProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class GenerationStubClient {

    private final AppProperties appProperties;

    /** 설정된 지연과 실패율로 이미지 생성 결과를 모사한다. */
    public String generate(String prompt) {
        AppProperties.Stub stub = appProperties.stub();
        sleep(randomDelayMillis(stub));

        if (ThreadLocalRandom.current().nextDouble() < stub.failureRate()) {
            log.info("stub generation failed: prompt={}", prompt);
            throw new StubGenerationException(prompt);
        }

        String resultUrl = "https://stub-images.local/" + UUID.randomUUID() + ".png";
        log.info("stub generation succeeded: prompt={}, resultUrl={}", prompt, resultUrl);
        return resultUrl;
    }

    /** 설정 범위에서 임의 지연 시간을 선택한다. */
    private long randomDelayMillis(AppProperties.Stub stub) {
        if (stub.maxDelayMillis() <= stub.minDelayMillis()) {
            return stub.minDelayMillis();
        }
        return ThreadLocalRandom.current().nextLong(stub.minDelayMillis(), stub.maxDelayMillis() + 1);
    }

    /** 지정 시간 동안 현재 스레드를 대기시킨다. */
    private void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("stub 지연 중 인터럽트 발생", e);
        }
    }
}
