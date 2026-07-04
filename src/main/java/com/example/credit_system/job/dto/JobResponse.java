package com.example.credit_system.job.dto;

import com.example.credit_system.job.domain.Job;

import java.time.Instant;

public record JobResponse(Long id, String status, int attemptNo, long holdAmount,
                          String prompt, String resultUrl, Instant updatedAt) {

    public static JobResponse from(Job job) {
        return new JobResponse(job.getId(), job.getStatus().name(), job.getAttemptNo(),
                job.getHoldAmount(), job.getPrompt(), job.getResultUrl(), job.getUpdatedAt());
    }
}
