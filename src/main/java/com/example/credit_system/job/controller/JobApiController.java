package com.example.credit_system.job.controller;

import com.example.credit_system.job.dto.JobCreateRequest;
import com.example.credit_system.job.dto.JobResponse;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.dto.HoldResult;
import com.example.credit_system.job.service.HoldService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/jobs")
public class JobApiController {

    private final HoldService holdService;
    private final JobRepository jobRepository;

    @PostMapping
    public HoldResult create(@RequestHeader("X-Organization-Id") Long organizationId,
                             @RequestBody JobCreateRequest request) {
        return holdService.requestGeneration(organizationId, request.idemKey(), request.prompt());
    }

    @GetMapping
    public List<JobResponse> list(@RequestHeader("X-Organization-Id") Long organizationId) {
        return jobRepository.findByOrganizationIdOrderByIdDesc(organizationId).stream()
                .map(JobResponse::from)
                .toList();
    }
}
