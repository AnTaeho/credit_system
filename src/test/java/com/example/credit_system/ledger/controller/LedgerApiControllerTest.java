package com.example.credit_system.ledger.controller;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
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
import org.springframework.boot.http.client.HttpRedirects;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LedgerApiControllerTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired UserRepository userRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired PasswordEncoder passwordEncoder;

    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 1000L));
        userRepository.save(new User(organization.getId(), "alice", passwordEncoder.encode("secret123")));
        ledgerRepository.save(LedgerEntry.of(organization.getId(), 1L, LedgerType.HOLD, -100L));
        ledgerRepository.save(LedgerEntry.of(organization.getId(), null, LedgerType.CHARGE, 500L));
    }

    @AfterEach
    void tearDown() {
        ledgerRepository.deleteAll();
        userRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 세션이_있으면_ledger_내역을_최신순으로_돌려준다() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, loginAndGetSessionCookie());

        ResponseEntity<LedgerResponse[]> response = restTemplate.exchange(
                url("/api/ledger"), HttpMethod.GET, new HttpEntity<>(headers), LedgerResponse[].class);

        List<LedgerResponse> entries = List.of(response.getBody());
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).type()).isEqualTo("CHARGE");
        assertThat(entries.get(1).type()).isEqualTo("HOLD");
    }

    @Test
    void 세션_없이_호출하면_401이다() {
        ResponseEntity<String> response = restTemplate.getForEntity(url("/api/ledger"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    private String loginAndGetSessionCookie() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", "alice");
        form.add("password", "secret123");

        ResponseEntity<Void> response = restTemplate.withRedirects(HttpRedirects.DONT_FOLLOW).exchange(
                url("/login"), HttpMethod.POST, new HttpEntity<>(form, headers), Void.class);

        return response.getHeaders().get(HttpHeaders.SET_COOKIE).get(0).split(";")[0];
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
