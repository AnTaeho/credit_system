package com.example.credit_system.ledger.controller;

import com.example.credit_system.ledger.dto.LedgerResponse;
import com.example.credit_system.ledger.repository.LedgerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/ledger")
public class LedgerApiController {

    private final LedgerRepository ledgerRepository;

    /** 요청 조직의 원장 내역을 최신순으로 반환한다. */
    @GetMapping
    public List<LedgerResponse> list(@RequestHeader("X-Organization-Id") Long organizationId) {
        return ledgerRepository.findByOrganizationIdOrderByIdDesc(organizationId).stream()
                .map(LedgerResponse::from)
                .toList();
    }
}
