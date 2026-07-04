package com.example.credit_system.job.controller;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.job.dto.JobCreateRequest;
import com.example.credit_system.job.dto.JobCreateResponse;
import com.example.credit_system.job.dto.JobResponse;
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
class JobApiControllerTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    UserRepository userRepository;

    @Autowired
    OrganizationRepository organizationRepository;

    @Autowired
    PasswordEncoder passwordEncoder;

    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 1000L));
        userRepository.save(new User(organization.getId(), "alice", passwordEncoder.encode("secret123")));
    }

    @AfterEach
    void tearDown() {
        userRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 세션_없이_호출하면_401이다() {
        ResponseEntity<String> response = restTemplate.getForEntity(url("/api/jobs"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void 로그인_후_생성_요청과_목록_조회가_정상_동작한다() {
        String sessionCookie = loginAndGetSessionCookie();
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, sessionCookie);
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<JobCreateResponse> createResponse = restTemplate.exchange(
                url("/api/jobs"), HttpMethod.POST,
                new HttpEntity<>(new JobCreateRequest("idem-1", "a cat"), headers),
                JobCreateResponse.class);

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(createResponse.getBody().duplicate()).isFalse();

        ResponseEntity<JobResponse[]> listResponse = restTemplate.exchange(
                url("/api/jobs"), HttpMethod.GET, new HttpEntity<>(headers), JobResponse[].class);

        List<JobResponse> jobs = List.of(listResponse.getBody());
        assertThat(jobs).hasSize(1);
        assertThat(jobs.get(0).status()).isEqualTo("HOLDING");
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
