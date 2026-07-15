package com.example.credit_system.job.controller;

import com.example.credit_system.global.auth.SessionConst;
import com.example.credit_system.job.dto.JobCreateRequest;
import com.example.credit_system.job.dto.JobCreateResponse;
import com.example.credit_system.job.dto.JobResponse;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.HoldResult;
import com.example.credit_system.job.service.HoldService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/jobs")
public class JobApiController {

    private final HoldService holdService;
    private final JobRepository jobRepository;

    /** 생성 작업을 접수한다. */
    @PostMapping
    public JobCreateResponse create(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId,
                                    @RequestBody JobCreateRequest request) {
        HoldResult result = holdService.requestGeneration(organizationId, request.idemKey(), request.prompt());
        return new JobCreateResponse(result.jobId(), result.duplicate());
    }

    /** 로그인 조직의 작업 목록을 반환한다. */
    @GetMapping
    public List<JobResponse> list(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId) {
        return jobRepository.findByOrganizationIdOrderByIdDesc(organizationId).stream()
                .map(JobResponse::from)
                .toList();
    }
}
