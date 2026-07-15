package com.example.credit_system.organization.service;

import com.example.credit_system.global.exception.InvalidRequestException;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void 충전_금액이_0_이하면_거부한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        assertThatThrownBy(() -> chargeService.charge(organization.getId(), 0L))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("amount는 0보다 커야 합니다.");

        assertThat(organizationRepository.findById(organization.getId()).orElseThrow().getBalance())
                .isEqualTo(500L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).isEmpty();
    }

    @Test
    void 충전_금액이_상한을_초과하면_거부한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        assertThatThrownBy(() -> chargeService.charge(organization.getId(), 1_000_001L))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("amount는 1,000,000을 초과할 수 없습니다.");

        assertThat(organizationRepository.findById(organization.getId()).orElseThrow().getBalance())
                .isEqualTo(500L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).isEmpty();
    }
}
