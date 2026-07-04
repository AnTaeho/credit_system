package com.example.credit_system.outbox.service;

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

    public void write(Long jobId, Long organizationId, int attemptNo, String prompt) {
        GenerationJobMessage message = new GenerationJobMessage(jobId, organizationId, attemptNo, prompt);
        String payload = objectMapper.writeValueAsString(message);
        outboxRepository.save(new OutboxEntry(jobId, payload));
    }
}
