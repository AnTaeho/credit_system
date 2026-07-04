package com.example.credit_system.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Getter
@Table(name = "ledger_entries")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long organizationId;

    private Long jobId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private LedgerType type;

    @Column(nullable = false)
    private long amount;

    @Column(nullable = false)
    private Instant createdAt;

    private LedgerEntry(Long organizationId, Long jobId, LedgerType type, long amount) {
        this.organizationId = organizationId;
        this.jobId = jobId;
        this.type = type;
        this.amount = amount;
        this.createdAt = Instant.now();
    }

    public static LedgerEntry of(Long organizationId, Long jobId, LedgerType type, long amount) {
        return new LedgerEntry(organizationId, jobId, type, amount);
    }
}
