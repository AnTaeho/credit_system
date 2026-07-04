package com.example.credit_system.job.concurrency;

import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.HoldResult;
import com.example.credit_system.job.service.HoldService;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = "generation-jobs")
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.scheduling.enabled=true",
        "app.worker.enabled=true",
        "app.stub.failure-rate=1.0",
        "app.scheduling.outbox-relay-interval-millis=200",
        "app.scheduling.dead-job-scan-interval-millis=500"
})
class RetryRefundTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.datasource.driver-class-name", mysql::getDriverClassName);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired HoldService holdService;
    @Autowired JobRepository jobRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    @Test
    void 매번_실패하면_재시도를_모두_소진하고_최종적으로_환불된다() {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));

        HoldResult result = holdService.requestGeneration(organization.getId(), "retry-key", "cat");

        // app.generation.max-attempts=3, 조건이 "attemptNo < 3"이므로 attemptNo 0,1,2인 시점에 각각 재시도가 한 번씩 더 일어나
        // 실제로는 초기 시도 포함 총 4번(attemptNo 0→1→2→3) 시도된 뒤 attemptNo=3에서 최종 환불된다.
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            var job = jobRepository.findById(result.jobId()).orElseThrow();
            assertThat(job.getStatus()).isEqualTo(JobStatus.REFUNDED);
            assertThat(job.getAttemptNo()).isEqualTo(3);
        });

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(1000L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .extracting(entry -> entry.getType().name())
                .contains("HOLD", "REFUND");
    }
}
