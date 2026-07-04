package com.example.credit_system.auth.repository;

import com.example.credit_system.auth.domain.User;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class UserRepositoryTest {

    @Autowired
    UserRepository userRepository;

    @Test
    void username으로_조회한다() {
        userRepository.save(new User(1L, "alice", "encoded-password"));

        User found = userRepository.findByUsername("alice").orElseThrow();

        assertThat(found.getOrganizationId()).isEqualTo(1L);
        assertThat(found.getPassword()).isEqualTo("encoded-password");
    }

    @Test
    void 없는_username은_빈_Optional을_반환한다() {
        assertThat(userRepository.findByUsername("ghost")).isEmpty();
    }
}
