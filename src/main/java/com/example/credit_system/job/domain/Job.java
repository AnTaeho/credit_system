package com.example.credit_system.job.domain;

import com.example.credit_system.global.domain.BaseEntity;
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

@Entity
@Getter
@Table(name = "jobs")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Job extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long organizationId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private JobStatus status;

    @Column(nullable = false)
    private int attemptNo;

    @Column(nullable = false)
    private long holdAmount;

    @Column(nullable = false, length = 1000)
    private String prompt;

    private String resultUrl;

    /** 차감 대기 상태의 작업을 생성한다. */
    private Job(Long organizationId, long holdAmount, String prompt) {
        this.organizationId = organizationId;
        this.status = JobStatus.HOLDING;
        this.attemptNo = 0;
        this.holdAmount = holdAmount;
        this.prompt = prompt;
    }

    /** 차감 대기 작업을 생성한다. */
    public static Job hold(Long organizationId, long holdAmount, String prompt) {
        return new Job(organizationId, holdAmount, prompt);
    }
}
