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

    private long randomDelayMillis(AppProperties.Stub stub) {
        if (stub.maxDelayMillis() <= stub.minDelayMillis()) {
            return stub.minDelayMillis();
        }
        return ThreadLocalRandom.current().nextLong(stub.minDelayMillis(), stub.maxDelayMillis() + 1);
    }

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
