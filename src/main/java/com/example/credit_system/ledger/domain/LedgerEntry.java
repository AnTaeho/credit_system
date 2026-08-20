package com.example.credit_system.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Getter
@Table(name = "ledger_entries",
        indexes = @Index(name = "idx_ledger_org_id", columnList = "organizationId"),
        uniqueConstraints = @UniqueConstraint(name = "uk_ledger_org_idem", columnNames = {"organizationId", "idemKey"}))
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

    @Column(length = 100)
    private String idemKey;

    @Column(nullable = false)
    private Instant createdAt;

    private LedgerEntry(Long organizationId, Long jobId, LedgerType type, long amount, String idemKey) {
        this.organizationId = organizationId;
        this.jobId = jobId;
        this.type = type;
        this.amount = amount;
        this.idemKey = idemKey;
        this.createdAt = Instant.now();
    }

    public static LedgerEntry of(Long organizationId, Long jobId, LedgerType type, long amount) {
        if (type == LedgerType.CHARGE) {
            throw new IllegalArgumentException(
                    "CHARGE 타입은 멱등키 없이 생성할 수 없습니다. charge(organizationId, idemKey, amount)를 사용하세요.");
        }
        return new LedgerEntry(organizationId, jobId, type, amount, null);
    }

    public static LedgerEntry charge(Long organizationId, String idemKey, long amount) {
        if (idemKey == null || idemKey.isBlank()) {
            throw new IllegalArgumentException("CHARGE 원장은 idemKey가 비어 있으면 안 됩니다: idemKey=" + idemKey);
        }
        return new LedgerEntry(organizationId, null, LedgerType.CHARGE, amount, idemKey);
    }
}
