package com.example.credit_system.job.dto;

import com.example.credit_system.job.domain.Job;

import java.time.Instant;

public record JobResponse(Long id, String status, int attemptNo, long holdAmount,
                          String prompt, String resultUrl, Instant updatedAt) {

    /** 작업 엔티티를 응답 객체로 변환한다. */
    public static JobResponse from(Job job) {
        return new JobResponse(job.getId(), job.getStatus().name(), job.getAttemptNo(),
                job.getHoldAmount(), job.getPrompt(), job.getResultUrl(), job.getUpdatedAt());
    }
}
