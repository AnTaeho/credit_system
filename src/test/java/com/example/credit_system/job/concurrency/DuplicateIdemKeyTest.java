package com.example.credit_system.job.concurrency;

import com.example.credit_system.global.exception.DuplicateRequestInProgressException;
import com.example.credit_system.job.repository.JobRepository;
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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class DuplicateIdemKeyTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit");

    @DynamicPropertySource
    static void datasourceProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.datasource.driver-class-name", mysql::getDriverClassName);
    }

    @Autowired HoldService holdService;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired OrganizationRepository organizationRepository;

    @Test
    void 동일_idemKey로_동시_요청해도_job과_차감은_한_번만_일어난다() throws InterruptedException {
        Organization organization = organizationRepository.save(new Organization("acme", 10_000L));
        String idemKey = "shared-key";

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    holdService.requestGeneration(organization.getId(), idemKey, "cat");
                } catch (DuplicateRequestInProgressException e) {
                    // 다른 스레드가 idempotency key를 선점한 직후, job 연결 전의 극히 짧은 race window에 걸린 경우.
                    // 클라이언트가 같은 idemKey로 재시도하면 결국 성공 응답을 받게 되므로 이 테스트에선 무시한다.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        start.countDown();
        done.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(jobRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).hasSize(1);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).hasSize(1);

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(10_000L - 100L);
    }
}
