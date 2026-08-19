package com.example.credit_system.benchmark;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("benchmark")
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@Import(BalanceStrategyBenchmark.BenchmarkTestConfig.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BalanceStrategyBenchmark {

    private static final long ACCOUNT_ID = 1L;
    private static final long AMOUNT = 100L;

    private static final int DEFAULT_REQUESTS = 5000;
    private static final int DEFAULT_WARMUP = 500;
    private static final int[] DEFAULT_CONCURRENCY = {1, 10, 50, 100};

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit")
            .withCommand("--max-connections=200");

    @DynamicPropertySource
    static void datasourceProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.datasource.driver-class-name", mysql::getDriverClassName);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> 128);
    }

    @TestConfiguration
    static class BenchmarkTestConfig {

        @Bean
        TransactionTemplate benchTransactionTemplate(PlatformTransactionManager transactionManager) {
            return new TransactionTemplate(transactionManager);
        }

        @Bean
        TransactionTemplate benchRequiresNewTransactionTemplate(PlatformTransactionManager transactionManager) {
            TransactionTemplate template = new TransactionTemplate(transactionManager);
            template.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRES_NEW);
            return template;
        }

        @Bean
        ConditionalUpdateStrategy conditionalUpdateStrategy(JdbcTemplate jdbcTemplate,
                                                              TransactionTemplate benchTransactionTemplate) {
            return new ConditionalUpdateStrategy(jdbcTemplate, benchTransactionTemplate);
        }

        @Bean
        PessimisticLockStrategy pessimisticLockStrategy(JdbcTemplate jdbcTemplate,
                                                          TransactionTemplate benchTransactionTemplate) {
            return new PessimisticLockStrategy(jdbcTemplate, benchTransactionTemplate);
        }

        @Bean
        OptimisticLockStrategy optimisticLockStrategy(JdbcTemplate jdbcTemplate,
                                                        TransactionTemplate benchRequiresNewTransactionTemplate) {
            return new OptimisticLockStrategy(jdbcTemplate, benchRequiresNewTransactionTemplate);
        }
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ConditionalUpdateStrategy conditionalUpdateStrategy;

    @Autowired
    PessimisticLockStrategy pessimisticLockStrategy;

    @Autowired
    OptimisticLockStrategy optimisticLockStrategy;

    @BeforeAll
    static void createTable(@Autowired JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS bench_account (
                  id BIGINT PRIMARY KEY,
                  balance BIGINT NOT NULL,
                  version BIGINT NOT NULL DEFAULT 0
                )
                """);
    }

    @Test
    @Order(1)
    void runFullBenchmarkMatrix() {
        int requests = Integer.getInteger("bench.requests", DEFAULT_REQUESTS);
        int warmup = Integer.getInteger("bench.warmup", DEFAULT_WARMUP);
        int[] concurrencyLevels = parseConcurrency(System.getProperty("bench.concurrency"));

        long initialBalance = ((long) requests) * AMOUNT * 2;

        List<DeductStrategy> strategies = List.of(
                conditionalUpdateStrategy,
                pessimisticLockStrategy,
                optimisticLockStrategy
        );

        List<BenchmarkResult> results = new ArrayList<>();

        for (int concurrency : concurrencyLevels) {
            for (DeductStrategy strategy : strategies) {
                resetBalance(initialBalance);
                BenchmarkHarness.run(strategy, concurrency, warmup, ACCOUNT_ID, AMOUNT);

                resetBalance(initialBalance);
                BenchmarkResult result = BenchmarkHarness.run(strategy, concurrency, requests, ACCOUNT_ID, AMOUNT);
                results.add(result);

                verifyConsistency(result, initialBalance, requests);
            }
        }

        printMarkdownTable(results);
    }

    private void resetBalance(long initialBalance) {
        int updated = jdbcTemplate.update(
                "UPDATE bench_account SET balance = ?, version = 0 WHERE id = ?",
                initialBalance, ACCOUNT_ID);
        if (updated == 0) {
            jdbcTemplate.update(
                    "INSERT INTO bench_account (id, balance, version) VALUES (?, ?, 0)",
                    ACCOUNT_ID, initialBalance);
        }
    }

    private void verifyConsistency(BenchmarkResult result, long initialBalance, int requests) {
        Long finalBalance = jdbcTemplate.queryForObject(
                "SELECT balance FROM bench_account WHERE id = ?", Long.class, ACCOUNT_ID);
        long expectedBalance = initialBalance - ((long) result.successCount() * AMOUNT);

        assertThat(finalBalance)
                .as("[%s @ concurrency=%d] final balance must equal initial - success*cost",
                        result.strategy(), result.concurrency())
                .isEqualTo(expectedBalance);

        assertThat(result.successCount() + result.failureCount())
                .as("[%s @ concurrency=%d] success + failure must equal total requests",
                        result.strategy(), result.concurrency())
                .isEqualTo(requests);
    }

    private static int[] parseConcurrency(String property) {
        if (property == null || property.isBlank()) {
            return DEFAULT_CONCURRENCY;
        }
        return Arrays.stream(property.split(","))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    private static void printMarkdownTable(List<BenchmarkResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("| strategy | concurrency | tps | p50 (µs) | p95 (µs) | p99 (µs) | success | failure | retries |\n");
        sb.append("|---|---|---|---|---|---|---|---|---|\n");
        for (BenchmarkResult r : results) {
            sb.append(String.format(
                    "| %s | %d | %.1f | %d | %d | %d | %d | %d | %d |%n",
                    r.strategy(), r.concurrency(), r.tps(), r.p50Micros(), r.p95Micros(), r.p99Micros(),
                    r.successCount(), r.failureCount(), r.totalRetries()));
        }
        System.out.println(sb);
    }
}
