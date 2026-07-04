# Credit System

Organization이 공유하는 크레딧을 선결제/차감하고, 비동기 이미지 생성(stub) 실패 시 정확히 환불하는 것을 목표로 한 포트폴리오 프로젝트입니다.
핵심 설계 원칙과 흐름은 [`docs/superpowers/specs/2026-07-04-credit-system-design.md`](docs/superpowers/specs/2026-07-04-credit-system-design.md)에 정리돼 있습니다.

이미지 생성 자체는 이 프로젝트의 관심사가 아니라서 지연+확률적 실패를 가진 stub(`GenerationStubClient`)으로 대체돼 있습니다.
핵심은 "크레딧이 항상 정확하게 차감·환불되는가"이며, 이는 조건부 UPDATE(낙관적 락 + fencing token)와
[`docs/superpowers/plans/2026-07-04-credit-system.md`](docs/superpowers/plans/2026-07-04-credit-system.md)의 Task 17/18에 있는 Testcontainers 기반 동시성 테스트로 증명합니다.

## 기술 스택

Spring Boot 4.1 (Java 17) / Spring Data JPA / Spring Kafka / Spring Data Redis / Thymeleaf / MySQL(운영) / H2(테스트) / Testcontainers / EmbeddedKafka

## 로컬 실행

```bash
docker compose up -d          # MySQL, Kafka, Redis
./gradlew bootRun             # http://localhost:8080
```

데모 계정 (최초 기동 시 `DataSeeder`가 자동 생성):

| username | password | organization | 초기 잔액 |
|---|---|---|---|
| alice | password123 | Acme Corp | 10,000 |
| bob | password123 | Globex Inc | 5,000 |

`/login`으로 로그인하면 `/dashboard`에서 잔액 확인, 충전, 이미지 생성 요청, job 상태(3초 폴링), ledger 내역을 볼 수 있습니다.

## 멱등성(중복 요청 방지) 확인해보기

같은 `idemKey`로 재전송하면 새 job을 만들지 않고 기존 job을 그대로 반환합니다 (`duplicate: true`).

```bash
# 1) 로그인해서 세션 쿠키 저장
curl -c cookies.txt -X POST http://localhost:8080/login \
  -d "username=alice&password=password123"

# 2) 최초 요청
curl -b cookies.txt -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"idemKey":"demo-key-1","prompt":"a cat wearing sunglasses"}'
# → {"jobId":1,"duplicate":false}

# 3) 같은 idemKey로 재전송 (네트워크 재시도 시뮬레이션)
curl -b cookies.txt -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"idemKey":"demo-key-1","prompt":"a cat wearing sunglasses"}'
# → {"jobId":1,"duplicate":true}  (같은 jobId, 잔액도 한 번만 차감됨)
```

## 테스트 실행

```bash
./gradlew test
```

Mock 프레임워크는 어디에도 쓰지 않습니다. 가벼운 테스트는 H2, 무거운 테스트(동시성·E2E 파이프라인)는 Docker가 필요한
Testcontainers(MySQL, Redis)와 EmbeddedKafka를 사용합니다 — 로컬에 Docker가 떠 있어야 전체 테스트가 통과합니다.

## 알려진 한계 (향후 과제)

- 대형 Organization의 트래픽 집중 시 balance row 잠금 경합 — 샤딩/Redis 원자적 카운터 검토 필요
- Outbox relay 발행 성공 후 sent 마킹 실패 시 중복 발행 가능 — 워커의 attempt_no 조건부 UPDATE가 안전망 역할
- Charge는 내부 잔액 증가만 구현 — 실제 PG 연동 없음
- 최종 환불 단계에서 organization 잔액 반영이 반복 충돌로 실패하면 ERROR 로그만 남기고 운영 알림은 없음
