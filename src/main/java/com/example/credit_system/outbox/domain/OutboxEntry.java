package com.example.credit_system.outbox.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
@Table(name = "outbox_entries")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long jobId;

    @Column(nullable = false, length = 2000)
    private String payload;

    @Column(nullable = false)
    private boolean sent;

    @Column(nullable = false)
    private Instant createdAt;

    public OutboxEntry(Long jobId, String payload) {
        this.jobId = jobId;
        this.payload = payload;
        this.sent = false;
        this.createdAt = Instant.now();
    }
}
