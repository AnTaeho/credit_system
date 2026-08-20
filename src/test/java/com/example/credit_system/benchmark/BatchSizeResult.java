package com.example.credit_system.benchmark;

public record BatchSizeResult(
        long durationMillis,
        int batchSize,
        int concurrency,
        double theoreticalTps,
        double observedTps,
        double idlePercent,
        double rowsReadPerCompletion,
        double wastedUpdatesPerCompletion
) {
}
