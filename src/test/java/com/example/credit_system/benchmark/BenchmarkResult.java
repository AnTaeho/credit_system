package com.example.credit_system.benchmark;

public record BenchmarkResult(
        String strategy,
        int concurrency,
        double tps,
        long p50Micros,
        long p95Micros,
        long p99Micros,
        int successCount,
        int failureCount,
        long totalRetries
) {
}
