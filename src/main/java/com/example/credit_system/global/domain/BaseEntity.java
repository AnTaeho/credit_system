package com.example.credit_system.global.domain;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import lombok.Getter;

import java.time.Instant;

@Getter
@MappedSuperclass
public abstract class BaseEntity {

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    /** 엔티티 생성 시각과 수정 시각을 초기화한다. */
    protected BaseEntity() {
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }
}
