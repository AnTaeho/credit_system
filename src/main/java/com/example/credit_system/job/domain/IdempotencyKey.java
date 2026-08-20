package com.example.credit_system.job.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
@Table(name = "idempotency_keys",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_idempotency_org_key",
                columnNames = {"organizationId", "idemKey"}),
        indexes = @Index(name = "idx_idem_created_at", columnList = "createdAt"))
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class IdempotencyKey {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long organizationId;

    @Column(nullable = false, length = 100)
    private String idemKey;

    private Long jobId;

    @Column(nullable = false)
    private Instant createdAt;

    public IdempotencyKey(Long organizationId, String idemKey) {
        this.organizationId = organizationId;
        this.idemKey = idemKey;
        this.createdAt = Instant.now();
    }
}
