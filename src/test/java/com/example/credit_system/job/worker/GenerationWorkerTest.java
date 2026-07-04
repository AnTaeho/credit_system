package com.example.credit_system.job.worker;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.outbox.domain.GenerationJobMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = "generation-jobs")
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.worker.enabled=true"
})
class GenerationWorkerTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProps(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired OrganizationRepository organizationRepository;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;

    @Test
    void 카프카_메시지를_소비하면_job이_완료되고_confirm_ledger가_남는다() {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 100L, "a cat"));

        String payload = objectMapper.writeValueAsString(
                new GenerationJobMessage(job.getId(), organization.getId(), job.getAttemptNo(), job.getPrompt()));
        kafkaTemplate.send("generation-jobs", job.getId().toString(), payload);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Job found = jobRepository.findById(job.getId()).orElseThrow();
            assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
            assertThat(found.getResultUrl()).isNotNull();
        });

        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .anyMatch(entry -> entry.getType().name().equals("CONFIRM"));
    }
}
