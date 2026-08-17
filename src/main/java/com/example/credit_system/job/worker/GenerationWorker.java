package com.example.credit_system.job.worker;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/** DB의 HOLDING 작업을 조회하고, 하나의 워커만 실행하도록 선점한다. */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class GenerationWorker {

    private final JobRepository jobRepository;
    private final GenerationJobProcessor jobProcessor;
    private final int batchSize;

    /** 작업 큐 의존성과 한 주기당 처리 상한을 구성한다. */
    public GenerationWorker(JobRepository jobRepository,
                            GenerationJobProcessor jobProcessor,
                            @Value("${app.worker.batch-size:20}") int batchSize) {
        this.jobRepository = jobRepository;
        this.jobProcessor = jobProcessor;
        this.batchSize = batchSize;
    }

    /**
     * DB 큐에서 설정된 개수만큼 대기 작업을 읽어 선점 후보로 넘긴다.
     *
     * 한 주기가 오래 걸리거나 작업 수가 급증해도 DB와 워커 스레드 사용량이 무한히 커지지 않는다.
     * 여러 인스턴스가 같은 목록을 읽어도 괜찮다. 실제 소유권은 아래 조건부 UPDATE가 결정한다.
     */
    @Scheduled(fixedDelayString = "${app.scheduling.worker-interval-millis:500}")
    public void processPendingJobs() {
        jobRepository.findByStatusOrderByIdAsc(JobStatus.HOLDING, PageRequest.of(0, batchSize))
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
