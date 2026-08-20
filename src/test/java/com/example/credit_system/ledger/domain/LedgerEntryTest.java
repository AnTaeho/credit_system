package com.example.credit_system.ledger.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LedgerEntryTest {

    @Test
    void of에_CHARGE_타입을_넣으면_예외가_발생한다() {
        assertThatThrownBy(() -> LedgerEntry.of(1L, 10L, LedgerType.CHARGE, 500L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("charge(");
    }

    @ParameterizedTest
    @EnumSource(value = LedgerType.class, names = {"HOLD", "CONFIRM", "REFUND"})
    void of에_CHARGE가_아닌_타입은_정상_생성되고_idemKey가_없다(LedgerType type) {
        LedgerEntry entry = LedgerEntry.of(1L, 10L, type, -100L);

        assertThat(entry.getType()).isEqualTo(type);
        assertThat(entry.getJobId()).isEqualTo(10L);
        assertThat(entry.getIdemKey()).isNull();
    }

    @Test
    void charge는_jobId가_없고_idemKey가_채워진다() {
        LedgerEntry entry = LedgerEntry.charge(1L, "idem-key-1", 500L);

        assertThat(entry.getType()).isEqualTo(LedgerType.CHARGE);
        assertThat(entry.getJobId()).isNull();
        assertThat(entry.getIdemKey()).isEqualTo("idem-key-1");
    }

    @Test
    void charge에_idemKey가_null이면_예외가_발생한다() {
        assertThatThrownBy(() -> LedgerEntry.charge(1L, null, 500L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void charge에_idemKey가_공백이면_예외가_발생한다() {
        assertThatThrownBy(() -> LedgerEntry.charge(1L, "   ", 500L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
