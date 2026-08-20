package com.example.credit_system.organization.service;

import com.example.credit_system.global.exception.InvalidRequestException;
import com.example.credit_system.global.exception.OrganizationNotFoundException;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.dto.ChargeResponse;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

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

        ChargeResponse response = chargeService.charge(organization.getId(), "idem-1", 300L);

        assertThat(response.balance()).isEqualTo(800L);
        assertThat(response.duplicate()).isFalse();
        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(800L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .anyMatch(entry -> entry.getType().name().equals("CHARGE"));
    }

    @Test
    void 같은_idemKey로_두_번_충전해도_잔액은_한_번만_오른다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));
        String idemKey = "idem-dup";

        ChargeResponse first = chargeService.charge(organization.getId(), idemKey, 300L);
        ChargeResponse second = chargeService.charge(organization.getId(), idemKey, 300L);

        assertThat(first.duplicate()).isFalse();
        assertThat(first.balance()).isEqualTo(800L);
        assertThat(second.duplicate()).isTrue();
        assertThat(second.balance()).isEqualTo(800L);

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(800L);

        List<LedgerEntry> entries = ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId());
        assertThat(entries).filteredOn(entry -> entry.getType().name().equals("CHARGE")).hasSize(1);
    }

    @Test
    void 다른_idemKey면_각각_충전된다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        ChargeResponse first = chargeService.charge(organization.getId(), "idem-a", 300L);
        ChargeResponse second = chargeService.charge(organization.getId(), "idem-b", 200L);

        assertThat(first.duplicate()).isFalse();
        assertThat(second.duplicate()).isFalse();
        assertThat(second.balance()).isEqualTo(1000L);

        List<LedgerEntry> entries = ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId());
        assertThat(entries).filteredOn(entry -> entry.getType().name().equals("CHARGE")).hasSize(2);
    }

    @Test
    void idemKey가_없으면_거부한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        assertThatThrownBy(() -> chargeService.charge(organization.getId(), null, 300L))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("idemKey는 필수입니다.");
    }

    @Test
    void idemKey가_공백이면_거부한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        assertThatThrownBy(() -> chargeService.charge(organization.getId(), "   ", 300L))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("idemKey는 필수입니다.");
    }

    @Test
    void idemKey가_100자를_초과하면_거부한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));
        String tooLong = "a".repeat(101);

        assertThatThrownBy(() -> chargeService.charge(organization.getId(), tooLong, 300L))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("idemKey는 100자를 초과할 수 없습니다.");
    }

    @Test
    void 충전_금액이_0_이하면_거부한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        assertThatThrownBy(() -> chargeService.charge(organization.getId(), "idem-1", 0L))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("amount는 0보다 커야 합니다.");

        assertThat(organizationRepository.findById(organization.getId()).orElseThrow().getBalance())
                .isEqualTo(500L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).isEmpty();
    }

    @Test
    void 충전_금액이_상한을_초과하면_거부한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        assertThatThrownBy(() -> chargeService.charge(organization.getId(), "idem-1", 1_000_001L))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("amount는 1,000,000을 초과할 수 없습니다.");

        assertThat(organizationRepository.findById(organization.getId()).orElseThrow().getBalance())
                .isEqualTo(500L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).isEmpty();
    }

    @Test
    void 존재하지_않는_조직을_충전하면_예외가_발생한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));
        Long missingOrganizationId = organization.getId() + 999_999L;

        assertThatThrownBy(() -> chargeService.charge(missingOrganizationId, "idem-1", 300L))
                .isInstanceOf(OrganizationNotFoundException.class)
                .hasMessage("존재하지 않는 organization: " + missingOrganizationId);
    }
}
