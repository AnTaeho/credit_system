package com.example.credit_system.job.concurrency;

import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.organization.service.ChargeService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest
class ConcurrentChargeTest extends SharedContainers {

    @DynamicPropertySource
    static void datasourceProps(DynamicPropertyRegistry registry) {
        registerDatabase(registry, "concurrent_charge");
    }

    @Autowired ChargeService chargeService;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired OrganizationRepository organizationRepository;

    @Test
    void 동일_idemKey로_동시_충전해도_잔액은_한_번만_오른다() throws InterruptedException {
        Organization organization = organizationRepository.save(new Organization("acme", 10_000L));
        String idemKey = "shared-charge-key";

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
                    chargeService.charge(organization.getId(), idemKey, 300L);
                } catch (DataIntegrityViolationException e) {
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

        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .filteredOn(entry -> entry.getType().name().equals("CHARGE"))
                .hasSize(1);

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(10_000L + 300L);
    }
}
