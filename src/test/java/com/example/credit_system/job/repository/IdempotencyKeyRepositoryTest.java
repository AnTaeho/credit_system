package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.IdempotencyKey;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ActiveProfiles("test")
@DataJpaTest
class IdempotencyKeyRepositoryTest {

    @Autowired
    IdempotencyKeyRepository idempotencyKeyRepository;

    @Test
    void 동일_조직_동일_키는_유니크_제약으로_거부된다() {
        idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(1L, "key-1"));

        assertThatThrownBy(() ->
                idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(1L, "key-1")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void 다른_조직은_같은_키를_사용할_수_있다() {
        idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(1L, "key-1"));
        idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(2L, "key-1"));

        assertThat(idempotencyKeyRepository.count()).isEqualTo(2);
    }

    @Test
    void attachJobId로_job을_연결할_수_있다() {
        idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(1L, "key-1"));

        int updated = idempotencyKeyRepository.attachJobId(1L, "key-1", 42L);

        IdempotencyKey found = idempotencyKeyRepository
                .findByOrganizationIdAndIdemKey(1L, "key-1").orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getJobId()).isEqualTo(42L);
    }
}
