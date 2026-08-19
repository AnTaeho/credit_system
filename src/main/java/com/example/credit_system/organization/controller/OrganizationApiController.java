package com.example.credit_system.organization.controller;

import com.example.credit_system.organization.dto.BalanceResponse;
import com.example.credit_system.organization.dto.ChargeRequest;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.organization.service.ChargeService;
import com.example.credit_system.global.exception.OrganizationNotFoundException;
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

    @GetMapping("/me/balance")
    public BalanceResponse myBalance(@RequestHeader("X-Organization-Id") Long organizationId) {
        Organization organization = organizationRepository.findById(organizationId)
                .orElseThrow(() -> new OrganizationNotFoundException(organizationId));
        return new BalanceResponse(organization.getBalance());
    }

    @PostMapping("/me/charge")
    public BalanceResponse charge(@RequestHeader("X-Organization-Id") Long organizationId,
                                  @RequestBody ChargeRequest request) {
        return new BalanceResponse(chargeService.charge(organizationId, request.amount()));
    }
}
