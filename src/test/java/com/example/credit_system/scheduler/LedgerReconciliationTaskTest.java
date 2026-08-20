package com.example.credit_system.scheduler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class LedgerReconciliationTaskTest {

    @Autowired
    OrganizationRepository organizationRepository;

    @Autowired
    LedgerRepository ledgerRepository;

    LedgerReconciliationTask task;
    Logger taskLogger;
    ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        task = new LedgerReconciliationTask(ledgerRepository);

        logAppender = new ListAppender<>();
        logAppender.start();
        taskLogger = (Logger) LoggerFactory.getLogger(LedgerReconciliationTask.class);
        taskLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        taskLogger.detachAppender(logAppender);
    }

    @Test
    void 원장과_잔액이_맞으면_아무_경보도_남기지_않는다() {
        organizationRepository.save(new Organization("acme", 1000L));

        task.reconcile();

        assertThat(errorLogs()).isEmpty();
    }

    @Test
    void 충전과_hold가_반영된_조직도_대사를_통과한다() {
        Organization org = organizationRepository.save(new Organization("acme", 1000L));
        ledgerRepository.save(LedgerEntry.of(org.getId(), null, LedgerType.CHARGE, 500L));
        ledgerRepository.save(LedgerEntry.of(org.getId(), null, LedgerType.HOLD, -100L));
        organizationRepository.addBalance(org.getId(), 400L, Instant.now());
        organizationRepository.flush();

        task.reconcile();

        assertThat(errorLogs()).isEmpty();
    }

    @Test
    void 잔액이_원장과_어긋나면_ERROR로_경보한다() {
        Organization org = organizationRepository.save(new Organization("acme", 1000L));
        ledgerRepository.save(LedgerEntry.of(org.getId(), null, LedgerType.CHARGE, 500L));
        organizationRepository.addBalance(org.getId(), 999L, Instant.now());
        organizationRepository.flush();

        task.reconcile();

        assertThat(errorLogs()).hasSize(1);
        assertThat(errorLogs().get(0).getFormattedMessage()).contains("organizationId=" + org.getId());
    }

    @Test
    void 원장_항목이_없는_조직도_검사_대상에_포함된다() {
        Organization org = organizationRepository.save(new Organization("acme", 1000L));
        organizationRepository.addBalance(org.getId(), 1L, Instant.now());
        organizationRepository.flush();

        task.reconcile();

        assertThat(errorLogs()).hasSize(1);
        assertThat(errorLogs().get(0).getFormattedMessage()).contains("organizationId=" + org.getId());
    }

    @Test
    void 배치_크기를_넘는_조직도_모두_검사한다() {
        int mismatchIndex = 119;
        Organization mismatchOrg = null;
        for (int i = 0; i < 205; i++) {
            Organization org = organizationRepository.save(new Organization("org-" + i, 1000L));
            if (i == mismatchIndex) {
                mismatchOrg = org;
            }
        }
        organizationRepository.addBalance(mismatchOrg.getId(), 1L, Instant.now());
        organizationRepository.flush();

        task.reconcile();

        assertThat(errorLogs()).hasSize(1);
        assertThat(errorLogs().get(0).getFormattedMessage()).contains("organizationId=" + mismatchOrg.getId());
    }

    private List<ILoggingEvent> errorLogs() {
        return logAppender.list.stream().filter(event -> event.getLevel() == Level.ERROR).toList();
    }
}
