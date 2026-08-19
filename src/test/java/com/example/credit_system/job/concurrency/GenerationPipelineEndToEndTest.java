package com.example.credit_system.job.concurrency;

import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.dto.HoldResult;
import com.example.credit_system.job.service.HoldService;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ActiveProfiles("test")
@SpringBootTest(properties = {
        "app.scheduling.enabled=true",
        "app.worker.enabled=true",
        "app.stub.failure-rate=0.0",
        "app.scheduling.worker-interval-millis=100"
})
class GenerationPipelineEndToEndTest extends SharedContainers {

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registerDatabase(registry, "pipeline_e2e");
    }

    @Autowired HoldService holdService;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired OrganizationRepository organizationRepository;

    @Test
    void hold_요청부터_컨펌까지_전체_파이프라인이_실제로_동작한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));

        HoldResult result = holdService.requestGeneration(
                organization.getId(), "e2e-key", "a cat wearing sunglasses");

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            var job = jobRepository.findById(result.jobId()).orElseThrow();
            assertThat(job.getStatus()).isEqualTo(JobStatus.COMPLETED);
            assertThat(job.getResultUrl()).isNotNull();
        });

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(900L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .extracting(entry -> entry.getType().name())
                .contains("HOLD", "CONFIRM");
    }
}
