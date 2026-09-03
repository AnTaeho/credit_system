package com.example.credit_system.scheduler;

import com.example.credit_system.global.config.IdempotencyProperties;
import com.example.credit_system.job.repository.IdempotencyKeyRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.scheduling", name = "enabled", havingValue = "true", matchIfMissing = true)
public class IdempotencyKeyCleanupTask {

    private static final int CLEANUP_BATCH_SIZE = 500;

    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final IdempotencyProperties idempotencyProperties;

    @Scheduled(fixedDelayString = "${app.scheduling.idempotency-cleanup-interval-millis:3600000}")
    public void cleanup() {
        Instant cutoff = Instant.now().minus(idempotencyProperties.retentionDays(), ChronoUnit.DAYS);
        List<Long> ids;
        do {
            ids = idempotencyKeyRepository.findIdsCreatedBefore(cutoff, PageRequest.of(0, CLEANUP_BATCH_SIZE));
            if (ids.isEmpty()) {
                break;
            }
            try {
                idempotencyKeyRepository.deleteByIdIn(ids);
            } catch (RuntimeException e) {
                log.warn("멱등키 정리 배치 삭제 실패, 이번 주기 중단", e);
                break;
            }
        } while (ids.size() == CLEANUP_BATCH_SIZE);
    }
}
