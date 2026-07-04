package com.example.credit_system.outbox.repository;

import com.example.credit_system.outbox.domain.OutboxEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEntry, Long> {

    List<OutboxEntry> findBySentFalseOrderByIdAsc();

    @Modifying(clearAutomatically = true)
    @Query("UPDATE OutboxEntry o SET o.sent = true WHERE o.id = :id")
    int markSent(@Param("id") Long id);
}
