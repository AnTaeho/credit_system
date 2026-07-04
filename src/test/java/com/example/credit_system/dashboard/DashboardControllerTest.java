package com.example.credit_system.dashboard;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
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

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DashboardControllerTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired UserRepository userRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired PasswordEncoder passwordEncoder;

    @BeforeEach
    void setUp() {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));
        userRepository.save(new User(organization.getId(), "alice", passwordEncoder.encode("secret123")));
    }

    @AfterEach
    void tearDown() {
        userRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 로그인_페이지는_200을_반환한다() {
        ResponseEntity<String> response = restTemplate.getForEntity(url("/login"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void 세션_없이_대시보드에_접근하면_로그인으로_리다이렉트된다() {
        ResponseEntity<String> response = restTemplate.withRedirects(HttpRedirects.DONT_FOLLOW)
                .getForEntity(url("/dashboard"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FOUND);
        assertThat(response.getHeaders().getLocation().getPath()).isEqualTo("/login");
    }

    @Test
    void 로그인_후_대시보드에_접근하면_사용자명이_노출된다() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, loginAndGetSessionCookie());

        ResponseEntity<String> response = restTemplate.exchange(
                url("/dashboard"), HttpMethod.GET, new HttpEntity<>(headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("alice");
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
