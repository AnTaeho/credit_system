package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.IdempotencyProperties;
import com.example.credit_system.job.domain.IdempotencyKey;
import com.example.credit_system.job.repository.IdempotencyKeyRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class IdempotencyKeyCleanupTaskTest {

    @Autowired
    IdempotencyKeyRepository idempotencyKeyRepository;

    IdempotencyKeyCleanupTask task;

    @BeforeEach
    void setUp() {
        task = new IdempotencyKeyCleanupTask(idempotencyKeyRepository, new IdempotencyProperties(7));
    }

    @Test
    void 보존_기간이_지난_키는_삭제된다() {
        IdempotencyKey key = idempotencyKeyRepository.save(new IdempotencyKey(1L, "old-key"));
        ReflectionTestUtils.setField(key, "createdAt", Instant.now().minus(8, ChronoUnit.DAYS));
        idempotencyKeyRepository.save(key);

        task.cleanup();

        assertThat(idempotencyKeyRepository.findById(key.getId())).isEmpty();
    }

    @Test
    void 보존_기간_안의_키는_남는다() {
        IdempotencyKey key = idempotencyKeyRepository.save(new IdempotencyKey(1L, "recent-key"));

        task.cleanup();

        assertThat(idempotencyKeyRepository.findById(key.getId())).isPresent();
    }

    @Test
    void 배치_크기를_넘는_키도_모두_삭제된다() {
        int count = 510;
        for (int i = 0; i < count; i++) {
            IdempotencyKey key = idempotencyKeyRepository.save(new IdempotencyKey(1L, "key-" + i));
            ReflectionTestUtils.setField(key, "createdAt", Instant.now().minus(8, ChronoUnit.DAYS));
            idempotencyKeyRepository.save(key);
        }

        task.cleanup();

        assertThat(idempotencyKeyRepository.count()).isZero();
    }

    @Test
    void 지울_키가_없으면_아무것도_지우지_않는다() {
        task.cleanup();

        assertThat(idempotencyKeyRepository.count()).isZero();
    }
}
