package com.example.credit_system.organization.controller;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.dto.BalanceResponse;
import com.example.credit_system.organization.dto.ChargeRequest;
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
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrganizationApiControllerTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired UserRepository userRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired PasswordEncoder passwordEncoder;

    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 500L));
        userRepository.save(new User(organization.getId(), "alice", passwordEncoder.encode("secret123")));
    }

    @AfterEach
    void tearDown() {
        userRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 잔액_조회와_충전이_정상_동작한다() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, loginAndGetSessionCookie());

        ResponseEntity<BalanceResponse> before = restTemplate.exchange(
                url("/api/organizations/me/balance"), HttpMethod.GET, new HttpEntity<>(headers), BalanceResponse.class);
        assertThat(before.getBody().balance()).isEqualTo(500L);

        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<BalanceResponse> after = restTemplate.exchange(
                url("/api/organizations/me/charge"), HttpMethod.POST,
                new HttpEntity<>(new ChargeRequest(300L), headers), BalanceResponse.class);

        assertThat(after.getBody().balance()).isEqualTo(800L);
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
