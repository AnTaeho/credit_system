# Credit System

Organization이 공유하는 크레딧을 선결제/차감하고, 비동기 이미지 생성(stub) 실패 시 정확히 환불하는 것을 목표로 한 포트폴리오 프로젝트입니다.
핵심 설계 원칙과 흐름은 [`credit_system_design.md`](credit_system_design.md)에, 프로덕션 준비도 검토와 남은 과제는 [`PRODUCTION_READINESS.md`](PRODUCTION_READINESS.md)에 정리돼 있습니다.

이미지 생성 자체는 이 프로젝트의 관심사가 아니라서 지연+확률적 실패를 가진 stub(`GenerationStubClient`)으로 대체돼 있습니다.
핵심은 "크레딧이 항상 정확하게 차감·환불되는가"이며, check-then-act 대신 **원자적 조건부 UPDATE**(확인+실행을 SQL 한 문장으로)와
attemptNo fencing으로 보장하고, Testcontainers 기반 동시성·E2E 테스트로 증명합니다.
outbox 패턴, poison message DLT 격리, heartbeat/DB 기반 dead-job reaper 등 신뢰성 장치도 갖추고 있습니다.

## 기술 스택

Spring Boot 4.1 (Java 17) / Spring Data JPA / Spring Kafka / Spring Data Redis / Thymeleaf / MySQL / H2(테스트) / Testcontainers / EmbeddedKafka

## 로컬 실행

MySQL은 로컬 설치본(localhost:3306)을 사용하고, Kafka와 Redis는 docker compose로 띄웁니다.

```bash
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS credit_system"   # 스키마는 ddl-auto가 생성
docker compose up -d          # Kafka, Redis
./gradlew bootRun             # http://localhost:8080
```

MySQL 접속 정보는 `src/main/resources/application.yml`의 `spring.datasource`를 로컬 환경에 맞게 수정하세요.

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

가벼운 테스트는 H2 인메모리 DB, 무거운 테스트(동시성·E2E 파이프라인·DLT 격리)는 Docker가 필요한
Testcontainers(MySQL, Redis)와 EmbeddedKafka를 사용합니다 — 로컬에 Docker가 떠 있어야 전체 테스트(58건)가 통과합니다.
통합 테스트가 기본이고, Mockito는 외부 I/O 경계를 끊어야 하는 소수의 단위 테스트에만 씁니다.

## 알려진 한계 (향후 과제)

- 로컬 데모 전용으로 운용하기로 결정해 보안·운영 하드닝(CSRF 방어, Flyway 마이그레이션, 시크릿 분리)은
  의도적으로 보류 중 — 상세는 [`PRODUCTION_READINESS.md`](PRODUCTION_READINESS.md) 참고
- 대형 Organization의 트래픽 집중 시 balance row 잠금 경합 — 샤딩/Redis 원자적 카운터 검토 필요
- Outbox relay 발행 성공 후 sent 마킹 실패 시 중복 발행 가능 — 워커의 attemptNo 조건부 UPDATE가 안전망 역할
- Charge는 내부 잔액 증가만 구현 — 실제 PG 연동 없음
