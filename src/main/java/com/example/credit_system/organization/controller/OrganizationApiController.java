package com.example.credit_system.organization.controller;

import com.example.credit_system.organization.dto.BalanceResponse;
import com.example.credit_system.organization.dto.ChargeRequest;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.organization.service.ChargeService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/organizations")
public class OrganizationApiController {

    private final OrganizationRepository organizationRepository;
    private final ChargeService chargeService;

    /** 요청 조직의 현재 잔액을 반환한다. */
    @GetMapping("/me/balance")
    public BalanceResponse myBalance(@RequestHeader("X-Organization-Id") Long organizationId) {
        Organization organization = organizationRepository.findById(organizationId).orElseThrow();
        return new BalanceResponse(organization.getBalance());
    }

    /** 요청 조직의 잔액을 충전한다. */
    @PostMapping("/me/charge")
    public BalanceResponse charge(@RequestHeader("X-Organization-Id") Long organizationId,
                                  @RequestBody ChargeRequest request) {
        chargeService.charge(organizationId, request.amount());
        Organization organization = organizationRepository.findById(organizationId).orElseThrow();
        return new BalanceResponse(organization.getBalance());
    }
}
