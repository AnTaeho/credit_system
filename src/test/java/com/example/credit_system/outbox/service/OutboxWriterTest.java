package com.example.credit_system.outbox.service;

import com.example.credit_system.global.exception.InvalidRequestException;
import com.example.credit_system.outbox.domain.OutboxEntry;
import com.example.credit_system.outbox.repository.OutboxRepository;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ActiveProfiles("test")
@DataJpaTest
class OutboxWriterTest {

    @Autowired
    OutboxRepository outboxRepository;

    @Test
    void write하면_직렬화된_payload가_미전송_상태로_저장된다() {
        OutboxWriter outboxWriter = new OutboxWriter(outboxRepository, new ObjectMapper());

        outboxWriter.write(10L, 1L, 0, "a cat wearing sunglasses");

        List<OutboxEntry> saved = outboxRepository.findBySentFalseOrderByIdAsc();
        assertThat(saved).hasSize(1);
        assertThat(saved.get(0).getJobId()).isEqualTo(10L);
        assertThat(saved.get(0).isSent()).isFalse();
        assertThat(saved.get(0).getPayload())
                .contains("\"jobId\":10")
                .contains("\"attemptNo\":0")
                .contains("\"prompt\":\"a cat wearing sunglasses\"");
    }

    @Test
    void 직렬화된_payload가_허용_길이를_초과하면_거부한다() {
        OutboxWriter outboxWriter = new OutboxWriter(outboxRepository, new ObjectMapper());
        // 제어문자는 JSON 직렬화 시 \\uXXXX(6자)로 이스케이프되어 길이가 크게 늘어난다.
        String longPrompt = "\u0001".repeat(2000);

        assertThatThrownBy(() -> outboxWriter.write(10L, 1L, 0, longPrompt))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("직렬화된 메시지가 허용 길이를 초과했습니다.");

        assertThat(outboxRepository.findBySentFalseOrderByIdAsc()).isEmpty();
    }
}
