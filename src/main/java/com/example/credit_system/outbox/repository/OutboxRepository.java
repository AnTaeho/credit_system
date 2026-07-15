package com.example.credit_system.outbox.repository;

import com.example.credit_system.outbox.domain.OutboxEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEntry, Long> {

    /** 미발송 항목을 오래된 순으로 조회한다. */
    List<OutboxEntry> findBySentFalseOrderByIdAsc();

    /** outbox 항목을 발송 완료로 변경한다. */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("UPDATE OutboxEntry o SET o.sent = true WHERE o.id = :id")
    int markSent(@Param("id") Long id);

    /** 해당 작업의 미발송 outbox 항목이 존재하는지 확인한다. */
    boolean existsByJobIdAndSentFalse(Long jobId);
}
