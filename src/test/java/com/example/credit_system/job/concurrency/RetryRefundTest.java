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
        "app.stub.failure-rate=1.0",
        "app.scheduling.worker-interval-millis=100",
        "app.scheduling.dead-job-scan-interval-millis=500"
})
class RetryRefundTest extends SharedContainers {

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registerDatabase(registry, "retry_refund");
    }

    @Autowired HoldService holdService;
    @Autowired JobRepository jobRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    @Test
    void 매번_실패하면_재시도를_모두_소진하고_최종적으로_환불된다() {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));

        HoldResult result = holdService.requestGeneration(organization.getId(), "retry-key", "cat");

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            var job = jobRepository.findById(result.jobId()).orElseThrow();
            assertThat(job.getStatus()).isEqualTo(JobStatus.REFUNDED);
            assertThat(job.getAttemptNo()).isEqualTo(2);
        });

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(1000L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .extracting(entry -> entry.getType().name())
                .contains("HOLD", "REFUND");
    }
}
