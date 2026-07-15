package com.example.credit_system.organization.controller;

import com.example.credit_system.global.auth.SessionConst;
import com.example.credit_system.organization.dto.BalanceResponse;
import com.example.credit_system.organization.dto.ChargeRequest;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.organization.service.ChargeService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/organizations")
public class OrganizationApiController {

    private final OrganizationRepository organizationRepository;
    private final ChargeService chargeService;

    /** 로그인 조직의 현재 잔액을 반환한다. */
    @GetMapping("/me/balance")
    public BalanceResponse myBalance(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId) {
        Organization organization = organizationRepository.findById(organizationId).orElseThrow();
        return new BalanceResponse(organization.getBalance());
    }

    /** 로그인 조직의 잔액을 충전한다. */
    @PostMapping("/me/charge")
    public BalanceResponse charge(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId,
                                  @RequestBody ChargeRequest request) {
        chargeService.charge(organizationId, request.amount());
        Organization organization = organizationRepository.findById(organizationId).orElseThrow();
        return new BalanceResponse(organization.getBalance());
    }
}
