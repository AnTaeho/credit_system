package com.example.credit_system.global.config;

import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest(properties = {
        "app.seed.enabled=true",
        // 다른 테스트 클래스들과 같은 이름의 H2 인메모리 DB(credit_test)를 공유하면 DB_CLOSE_DELAY=-1
        // 때문에 시드 데이터가 JVM 전체에 걸쳐 살아남아 다른 테스트의 "alice" 삽입과 충돌한다.
        // 이 테스트만 별도 이름의 DB를 써서 격리한다.
        "spring.datasource.url=jdbc:h2:mem:seed_test;MODE=MySQL;DB_CLOSE_DELAY=-1"
})
class DataSeederTest {

    @Autowired OrganizationRepository organizationRepository;
    @Autowired UserRepository userRepository;

    @Test
    void 기동_시_샘플_조직과_사용자가_생성된다() {
        assertThat(organizationRepository.count()).isGreaterThanOrEqualTo(2);
        assertThat(userRepository.findByUsername("alice")).isPresent();
        assertThat(userRepository.findByUsername("bob")).isPresent();
    }
}
