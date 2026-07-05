package com.example.credit_system.outbox.domain;

public record GenerationJobMessage(
        Long jobId,
        Long organizationId,
        int attemptNo,
        String prompt
) {
}
