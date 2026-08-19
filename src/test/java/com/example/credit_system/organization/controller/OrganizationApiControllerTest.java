package com.example.credit_system.organization.controller;

import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.dto.BalanceResponse;
import com.example.credit_system.organization.dto.ChargeRequest;
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
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrganizationApiControllerTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired OrganizationRepository organizationRepository;

    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 500L));
    }

    @AfterEach
    void tearDown() {
        organizationRepository.deleteAll();
    }

    @Test
    void 잔액_조회와_충전이_정상_동작한다() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Organization-Id", String.valueOf(organization.getId()));

        ResponseEntity<BalanceResponse> before = restTemplate.exchange(
                url("/api/organizations/me/balance"), HttpMethod.GET, new HttpEntity<>(headers), BalanceResponse.class);
        assertThat(before.getBody().balance()).isEqualTo(500L);

        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<BalanceResponse> after = restTemplate.exchange(
                url("/api/organizations/me/charge"), HttpMethod.POST,
                new HttpEntity<>(new ChargeRequest(300L), headers), BalanceResponse.class);

        assertThat(after.getBody().balance()).isEqualTo(800L);
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
