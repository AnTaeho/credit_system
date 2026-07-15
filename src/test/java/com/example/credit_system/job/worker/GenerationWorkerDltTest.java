package com.example.credit_system.job.worker;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.outbox.domain.GenerationJobMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"generation-jobs", "generation-jobs.DLT"})
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.worker.enabled=true"
})
class GenerationWorkerDltTest {

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
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;
    @Autowired EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void poison_메시지가_DLT로_격리되어_이후_정상_메시지_처리를_막지_않는다() {
        String poisonPayload = "이것은 JSON이 아닌 poison 메시지입니다";
        kafkaTemplate.send("generation-jobs", "poison-key", poisonPayload);

        Organization organization = organizationRepository.save(new Organization("acme", 1000L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 100L, "a cat"));
        String validPayload = objectMapper.writeValueAsString(
                new GenerationJobMessage(job.getId(), organization.getId(), job.getAttemptNo(), job.getPrompt()));
        kafkaTemplate.send("generation-jobs", job.getId().toString(), validPayload);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            Job found = jobRepository.findById(job.getId()).orElseThrow();
            assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
            assertThat(found.getResultUrl()).isNotNull();
        });

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                embeddedKafkaBroker, "dlt-verification-group", true);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps)
                .createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "generation-jobs.DLT");

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(consumer, "generation-jobs.DLT", Duration.ofSeconds(20));
        assertThat(dltRecord.value()).isEqualTo(poisonPayload);
        consumer.close();
    }
}
