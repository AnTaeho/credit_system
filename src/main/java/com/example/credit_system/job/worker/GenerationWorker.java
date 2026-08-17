package com.example.credit_system.job.worker;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/** DB의 HOLDING 작업을 조회하고, 하나의 워커만 실행하도록 선점한다. */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class GenerationWorker {

    private final JobRepository jobRepository;
    private final GenerationJobProcessor jobProcessor;

    /**
     * DB 큐에서 대기 중인 작업을 읽어 선점 후보로 넘긴다.
     *
     * 여러 인스턴스가 같은 목록을 읽어도 괜찮다. 실제 소유권은 아래 조건부 UPDATE가 결정한다.
     */
    @Scheduled(fixedDelayString = "${app.scheduling.worker-interval-millis:500}")
    public void processPendingJobs() {
        jobRepository.findByStatusOrderByIdAsc(JobStatus.HOLDING)
                .forEach(this::claimAndProcess);
    }

    /**
     * 최신 작업 상태를 다시 읽고, HOLDING 상태일 때만 PROCESSING으로 바꾼다.
     *
     * 반환값이 0이면 다른 워커가 먼저 선점했거나 이미 취소·완료된 것이므로 외부 호출을 하지 않는다.
     */
    void claimAndProcess(Job job) {
        int updated = jobRepository.startProcessingIfAttemptMatches(
                job.getId(), job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("다른 워커가 선점했거나 무효한 작업 무시: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return;
        }
        jobProcessor.process(job);
    }
}
