package com.example.credit_system.job.stub;

import com.example.credit_system.global.config.AppProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationStubClientTest {

    @Test
    void failureRate가_0이면_항상_성공하고_결과_URL을_반환한다() {
        GenerationStubClient client = new GenerationStubClient(
                new AppProperties(null, new AppProperties.Stub(0.0, 0, 0), null, null, null, null));

        String resultUrl = client.generate("a cat wearing sunglasses");

        assertThat(resultUrl).startsWith("https://stub-images.local/");
    }

    @Test
    void failureRate가_1이면_항상_실패한다() {
        GenerationStubClient client = new GenerationStubClient(
                new AppProperties(null, new AppProperties.Stub(1.0, 0, 0), null, null, null, null));

        assertThatThrownBy(() -> client.generate("a cat wearing sunglasses"))
                .isInstanceOf(StubGenerationException.class);
    }
}
