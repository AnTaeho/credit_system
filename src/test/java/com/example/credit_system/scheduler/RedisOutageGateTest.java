package com.example.credit_system.scheduler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.credit_system.global.config.AppProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RedisOutageGateTest {

    RedisOutageGate gate;
    MutableClock clock;
    Logger gateLogger;
    ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        AppProperties appProperties = new AppProperties(
                new AppProperties.Generation(100L, 3),
                null,
                new AppProperties.Heartbeat(10, 1, 60),
                new AppProperties.Processing(60));
        clock = new MutableClock(Instant.now());
        gate = new RedisOutageGate(appProperties, clock);

        logAppender = new ListAppender<>();
        logAppender.start();
        gateLogger = (Logger) LoggerFactory.getLogger(RedisOutageGate.class);
        gateLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        gateLogger.detachAppender(logAppender);
    }

    @Test
    void 임계치_전에는_억제_경보를_내지_않는다() {
        beginOutage();
        continueOutage(Duration.ofSeconds(55));

        assertThat(errorLogs()).isEmpty();
    }

    @Test
    void 억제가_임계치를_넘기면_ERROR로_경보한다() {
        beginOutage();
        continueOutage(Duration.ofSeconds(60));

        assertThat(errorLogs()).hasSize(1);
        assertThat(errorLogs().get(0).getFormattedMessage())
                .contains("60초")
                .contains("회수가 그동안 계속 억제");
    }

    @Test
    void 억제_경보는_임계치_주기로만_재발행된다() {
        beginOutage();
        continueOutage(Duration.ofSeconds(60));
        assertThat(errorLogs()).hasSize(1);

        continueOutage(Duration.ofSeconds(55));
        assertThat(errorLogs()).hasSize(1);

        continueOutage(Duration.ofSeconds(5));
        assertThat(errorLogs()).hasSize(2);
    }

    @Test
    void 유예가_풀리면_억제_상태가_리셋되고_회복을_남긴다() {
        beginOutage();
        continueOutage(Duration.ofSeconds(60));
        assertThat(errorLogs()).hasSize(1);

        clock.advance(Duration.ofSeconds(11));
        assertThat(gate.isInRecoveryGrace()).isFalse();
        assertThat(infoLogs()).hasSize(1);
        assertThat(infoLogs().get(0).getFormattedMessage()).contains("회수를 재개");

        beginOutage();
        continueOutage(Duration.ofSeconds(55));
        assertThat(errorLogs()).hasSize(1);

        continueOutage(Duration.ofSeconds(5));
        assertThat(errorLogs()).hasSize(2);
    }

    private void beginOutage() {
        gate.recordFailure();
    }

    private void continueOutage(Duration duration) {
        for (long elapsed = 0; elapsed < duration.getSeconds(); elapsed += 5) {
            clock.advance(Duration.ofSeconds(5));
            gate.recordFailure();
            gate.isInRecoveryGrace();
        }
    }

    private List<ILoggingEvent> errorLogs() {
        return logsAt(Level.ERROR);
    }

    private List<ILoggingEvent> infoLogs() {
        return logsAt(Level.INFO);
    }

    private List<ILoggingEvent> logsAt(Level level) {
        return logAppender.list.stream().filter(event -> event.getLevel() == level).toList();
    }
}
