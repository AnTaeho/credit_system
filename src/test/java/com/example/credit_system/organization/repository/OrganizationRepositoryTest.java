package com.example.credit_system.organization.repository;

import com.example.credit_system.organization.domain.Organization;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class OrganizationRepositoryTest {

    @Autowired
    OrganizationRepository organizationRepository;

    @Test
    void 잔액이_충분하면_차감된다() {
        Organization org = organizationRepository.save(new Organization("acme", 1000L));

        int updated = organizationRepository.deductBalance(org.getId(), 300L, Instant.now());
        organizationRepository.flush();

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getBalance()).isEqualTo(700L);
    }

    @Test
    void 잔액이_부족하면_0행이_반환되고_잔액이_변하지_않는다() {
        Organization org = organizationRepository.save(new Organization("acme", 100L));

        int updated = organizationRepository.deductBalance(org.getId(), 300L, Instant.now());

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isZero();
        assertThat(found.getBalance()).isEqualTo(100L);
    }

    @Test
    void 환불은_잔액을_되돌린다() {
        Organization org = organizationRepository.save(new Organization("acme", 700L));

        int updated = organizationRepository.addBalance(org.getId(), 300L, Instant.now());

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getBalance()).isEqualTo(1000L);
    }
}
