package com.example.credit_system.global.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AppPropertiesTest {

    @Test
    void 유효한_heartbeat_설정은_생성된다() {
        assertThatCode(() -> new AppProperties.Heartbeat(10, 5, 60)).doesNotThrowAnyException();
    }

    @Test
    void refresh_interval이_timeout_이상이면_거부한다() {
        assertThatThrownBy(() -> new AppProperties.Heartbeat(10, 15, 60))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refresh-interval-seconds")
                .hasMessageContaining("timeout-seconds");
    }

    @Test
    void refresh_interval이_timeout과_같아도_거부한다() {
        assertThatThrownBy(() -> new AppProperties.Heartbeat(10, 10, 60))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("작아야 합니다");
    }

    @Test
    void timeout이_1_미만이면_거부한다() {
        assertThatThrownBy(() -> new AppProperties.Heartbeat(0, 5, 60))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout-seconds는 1 이상");
    }

    @Test
    void refresh_interval이_1_미만이면_거부한다() {
        assertThatThrownBy(() -> new AppProperties.Heartbeat(10, 0, 60))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refresh-interval-seconds는 1 이상");
    }

    @Test
    void suppression_alert가_1_미만이면_거부한다() {
        assertThatThrownBy(() -> new AppProperties.Heartbeat(10, 5, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("suppression-alert-seconds는 1 이상");
    }
}
