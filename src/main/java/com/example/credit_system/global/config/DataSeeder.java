package com.example.credit_system.global.config;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.seed", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DataSeeder implements CommandLineRunner {

    private final OrganizationRepository organizationRepository;
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    /** 저장소가 비어 있으면 샘플 조직과 사용자를 생성한다. */
    @Override
    public void run(String... args) {
        if (organizationRepository.count() > 0) {
            log.info("이미 시드 데이터가 존재해 스킵함");
            return;
        }

        Organization acme = organizationRepository.save(new Organization("Acme Corp", 10_000L));
        userRepository.save(new User(acme.getId(), "alice", passwordEncoder.encode("password123")));

        Organization globex = organizationRepository.save(new Organization("Globex Inc", 5_000L));
        userRepository.save(new User(globex.getId(), "bob", passwordEncoder.encode("password123")));

        log.info("샘플 데이터 시드 완료: alice/Acme Corp(10000), bob/Globex Inc(5000)");
    }
}
