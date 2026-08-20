package com.example.credit_system.ledger.controller;

import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.dto.LedgerResponse;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LedgerApiControllerTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 1000L));
        ledgerRepository.save(LedgerEntry.of(organization.getId(), 1L, LedgerType.HOLD, -100L));
        ledgerRepository.save(LedgerEntry.charge(organization.getId(), "charge-key-1", 500L));
    }

    @AfterEach
    void tearDown() {
        ledgerRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 조직_헤더가_있으면_ledger_내역을_최신순으로_돌려준다() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Organization-Id", String.valueOf(organization.getId()));

        ResponseEntity<LedgerResponse[]> response = restTemplate.exchange(
                url("/api/ledger"), HttpMethod.GET, new HttpEntity<>(headers), LedgerResponse[].class);

        List<LedgerResponse> entries = List.of(response.getBody());
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).type()).isEqualTo("CHARGE");
        assertThat(entries.get(1).type()).isEqualTo("HOLD");
    }

    @Test
    void 조직_헤더_없이_호출하면_400이다() {
        ResponseEntity<String> response = restTemplate.getForEntity(url("/api/ledger"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
