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
    void 버전이_일치하면_차감되고_버전이_증가한다() {
        Organization org = organizationRepository.save(new Organization("acme", 1000L));

        int updated = organizationRepository.deductBalance(org.getId(), 300L, 0L, Instant.now());
        organizationRepository.flush();

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getBalance()).isEqualTo(700L);
        assertThat(found.getVersion()).isEqualTo(1L);
    }

    @Test
    void 버전이_불일치하면_0행이_반환되고_잔액이_변하지_않는다() {
        Organization org = organizationRepository.save(new Organization("acme", 1000L));

        int updated = organizationRepository.deductBalance(org.getId(), 300L, 99L, Instant.now());

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isZero();
        assertThat(found.getBalance()).isEqualTo(1000L);
        assertThat(found.getVersion()).isZero();
    }

    @Test
    void 환불은_잔액을_되돌리고_버전을_증가시킨다() {
        Organization org = organizationRepository.save(new Organization("acme", 700L));

        int updated = organizationRepository.addBalance(org.getId(), 300L, 0L, Instant.now());

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getBalance()).isEqualTo(1000L);
        assertThat(found.getVersion()).isEqualTo(1L);
    }
}
