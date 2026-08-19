package com.example.credit_system.scheduler;

import com.example.credit_system.ledger.dto.LedgerBalanceCheck;
import com.example.credit_system.ledger.repository.LedgerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.scheduling", name = "enabled", havingValue = "true", matchIfMissing = true)
public class LedgerReconciliationTask {

    private static final int RECONCILE_BATCH_SIZE = 100;

    private final LedgerRepository ledgerRepository;

    @Scheduled(fixedDelayString = "${app.scheduling.reconciliation-interval-millis:60000}")
    public void reconcile() {
        int checkedCount = 0;
        int mismatchCount = 0;
        int page = 0;
        List<LedgerBalanceCheck> checks;
        do {
            checks = ledgerRepository.findBalanceChecks(PageRequest.of(page, RECONCILE_BATCH_SIZE));
            for (LedgerBalanceCheck check : checks) {
                try {
                    if (!reconcileOne(check)) {
                        mismatchCount++;
                    }
                    checkedCount++;
                } catch (RuntimeException e) {
                    log.warn("원장 대사 항목 처리 실패: organizationId={}", check.organizationId(), e);
                }
            }
            page++;
        } while (checks.size() == RECONCILE_BATCH_SIZE);
        log.info("원장 대사 주기 완료: checkedCount={}, mismatchCount={}", checkedCount, mismatchCount);
    }

    private boolean reconcileOne(LedgerBalanceCheck check) {
        long expected = check.initialBalance() + check.ledgerSum();
        if (expected == check.balance()) {
            return true;
        }
        log.error("원장 대사 불일치 발견: organizationId={}, balance={}, expected={}, initialBalance={}, ledgerSum={}, diff={}",
                check.organizationId(), check.balance(), expected, check.initialBalance(), check.ledgerSum(),
                check.balance() - expected);
        return false;
    }
}
