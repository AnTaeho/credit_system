package com.example.credit_system.organization.service;

import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class ChargeServiceTest {

    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    ChargeService chargeService;

    @BeforeEach
    void setUp() {
        chargeService = new ChargeService(organizationRepository, ledgerRepository);
    }

    @Test
    void 충전하면_잔액이_증가하고_ledger에_CHARGE가_남는다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        chargeService.charge(organization.getId(), 300L);

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(800L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .anyMatch(entry -> entry.getType().name().equals("CHARGE"));
    }
}
