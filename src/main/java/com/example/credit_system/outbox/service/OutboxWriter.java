package com.example.credit_system.outbox.service;

import com.example.credit_system.global.exception.InvalidRequestException;
import com.example.credit_system.outbox.domain.GenerationJobMessage;
import com.example.credit_system.outbox.domain.OutboxEntry;
import com.example.credit_system.outbox.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
public class OutboxWriter {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    /** 생성 작업 메시지를 outbox에 저장한다. */
    public void write(Long jobId, Long organizationId, int attemptNo, String prompt) {
        GenerationJobMessage message = new GenerationJobMessage(jobId, organizationId, attemptNo, prompt);
        String payload = objectMapper.writeValueAsString(message);
        if (payload.length() > OutboxEntry.MAX_PAYLOAD_LENGTH) {
            throw new InvalidRequestException("직렬화된 메시지가 허용 길이를 초과했습니다.");
        }
        outboxRepository.save(new OutboxEntry(jobId, payload));
    }
}
