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

    /** payload 컬럼에 저장 가능한 최대 길이(JSON 이스케이프 후 기준). */
    public static final int MAX_PAYLOAD_LENGTH = 8000;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long jobId;

    @Column(nullable = false, length = MAX_PAYLOAD_LENGTH)
    private String payload;

    @Column(nullable = false)
    private boolean sent;

    @Column(nullable = false)
    private Instant createdAt;

    /** 미발송 작업 메시지를 생성한다. */
    public OutboxEntry(Long jobId, String payload) {
        this.jobId = jobId;
        this.payload = payload;
        this.sent = false;
        this.createdAt = Instant.now();
    }
}
