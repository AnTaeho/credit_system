# PROCESSING 진입~heartbeat 등록 사이 크래시 / Redis 단일 장애점 (Major) — `d3cbe73`

상태 요약: [docs/PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md)

### 배경
워커는 job을 PROCESSING으로 전이시킨 뒤 Redis에 heartbeat를 등록하고, 스케줄러는
만료된 heartbeat(`findExpiredJobIds`)로 죽은 워커의 job을 감지한다.

### P — Problem
heartbeat 만료 감지만으로는 다음을 회수할 수 없다.
- ① PROCESSING 전이 직후 heartbeat 등록 전에 워커 크래시(등록된 적이 없어 만료도 없음).
- ② 등록 후 Redis 데이터 유실(단일 장애점).
- ③ DLT 격리된 메시지의 job이 PROCESSING에 남음(4번 연계).
→ 위 경우 job이 PROCESSING에 영구 정체.

### A — Analysis
Redis heartbeat는 "살아있음"의 신호일 뿐이라, 신호가 애초에 없거나 유실되면 감지 불가.
**Redis에 의존하지 않는 DB 기반 최종 안전망**이 필요하다. 단, 오탐(살아있는 워커를
죽었다고 오인)이 나도 크레딧 정합성은 깨지지 않아야 한다.

### Action
`DeadJobSchedulerTask.reapStaleProcessing()` 추가.

```java
Instant cutoff = Instant.now().minusSeconds(appProperties.processing().timeoutSeconds()); // 60s
for (Job job : jobRepository.findByStatusAndUpdatedAtBeforeOrderByIdAsc(JobStatus.PROCESSING, cutoff)) {
    if (heartbeatRegistry.hasLiveHeartbeat(job.getId())) continue;          // 살아있으면 스킵
    int updated = jobRepository.transitionIfStatusAndAttemptMatch(
            job.getId(), JobStatus.FAILED, JobStatus.PROCESSING, job.getAttemptNo(), Instant.now());
    if (updated == 1) { heartbeatRegistry.remove(job.getId()); ... }         // FAILED로 회수
}
```

`updatedAt`이 `app.processing.timeout-seconds`(60s)보다 오래됐고, 살아있는 heartbeat도
없는 PROCESSING job만 FAILED로 되돌려 재시도/환불 루프에 연결.

### R — Result
- ①②③ 모두 커버되어 PROCESSING 영구 정체가 사라짐.
- **오탐 안전성** — 워커가 실제로 살아있는데 오탐이 나도, 이후 retry가 attemptNo를
  올리므로 옛 워커의 최종 confirm(`complete`/`updateStatusIfAttemptMatches`)은 attemptNo
  불일치로 0행 처리되어 무효화된다. 최악의 경우 재작업일 뿐, 중복 차감 없음.
- 참고: 컨슈머 프로세스 크래시 자체는 오프셋 미커밋 → Kafka 재배송으로 복구되는 경로가
  이미 있고, reaper는 재배송으로 못 잡는 경우까지 막는 최종 안전망이다.
