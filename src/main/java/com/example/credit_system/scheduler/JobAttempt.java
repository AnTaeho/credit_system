package com.example.credit_system.scheduler;

import java.util.Optional;

public record JobAttempt(Long jobId, int attemptNo) {

    private static final String SEPARATOR = ":";

    String toMember() {
        return jobId + SEPARATOR + attemptNo;
    }

    static Optional<JobAttempt> parse(String member) {
        if (member == null) {
            return Optional.empty();
        }
        int separatorIndex = member.indexOf(SEPARATOR);
        if (separatorIndex < 0) {
            return Optional.empty();
        }
        try {
            Long jobId = Long.valueOf(member.substring(0, separatorIndex));
            int attemptNo = Integer.parseInt(member.substring(separatorIndex + 1));
            return Optional.of(new JobAttempt(jobId, attemptNo));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
