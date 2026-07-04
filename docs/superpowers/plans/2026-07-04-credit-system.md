# Credit System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 조건부 UPDATE 기반 동시성 안전 크레딧 시스템(hold/confirm/refund) + Kafka/Redis 비동기 파이프라인 + Thymeleaf 대시보드를 구현한다.

**Architecture:** 단일 Spring Boot 프로세스 안에 Web(Thymeleaf+REST), OutboxRelay(@Scheduled→Kafka), GenerationWorker(@KafkaListener), DeadJobSchedulerTask(@Scheduled, Redis heartbeat 감시)가 공존한다. 모든 상태 변경은 `@Modifying @Query` 조건부 UPDATE의 반영 행 수(0/1)로 판정한다 (JPA `@Version` 미사용).

**Tech Stack:** Spring Boot 4.1 / Java 17, Spring Data JPA, Spring Kafka, Spring Data Redis, Thymeleaf, Lombok, MySQL(운영·개발) / H2(테스트), Testcontainers, EmbeddedKafka, Awaitility

**Spec:** `docs/superpowers/specs/2026-07-04-credit-system-design.md` — 각 태스크 시작 전 해당 섹션을 반드시 읽을 것.

## Global Constraints

- 모든 PK는 `Long` + `@GeneratedValue(strategy = GenerationType.IDENTITY)`
- JPA `@Version` 금지 — 낙관적 락은 `@Modifying @Query` 조건부 UPDATE의 반영 행 수로 수동 판정
- **테스트에서 Mockito 등 mock 프레임워크 절대 금지.** 가벼운 테스트는 H2(`@DataJpaTest` + `@Import`), 무거운 테스트는 Testcontainers/EmbeddedKafka
- Lombok은 `@Getter`, `@RequiredArgsConstructor`, `@Slf4j`만 허용. `@Data`/`@Setter`/`@Builder` 금지
- 컨트롤러에 비즈니스 로직 금지 (라우팅+DTO 매핑만). 인터페이스는 구현체 2개 이상일 때만
- 매직 넘버는 상수 또는 `@ConfigurationProperties`로 추출
- 상태 전이(HOLD/CONFIRM/REFUND/재시도)는 `log.info`로 기록
- 베이스 패키지: `com.example.credit_system`
- 커밋 메시지 마지막 줄: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

## 전체 파일 구조 (완성 시점 기준)

```
src/main/java/com/example/credit_system/
├── CreditSystemApplication.java          (기존, @EnableScheduling 추가)
├── auth/
│   ├── controller/LoginController.java
│   ├── domain/User.java
│   └── repository/UserRepository.java
├── organization/
│   ├── controller/OrganizationApiController.java
│   ├── domain/Organization.java
│   ├── dto/BalanceResponse.java
│   ├── dto/ChargeRequest.java
│   ├── repository/OrganizationRepository.java
│   └── service/ChargeService.java
├── job/
│   ├── controller/JobApiController.java
│   ├── domain/Job.java
│   ├── domain/JobStatus.java
│   ├── domain/IdempotencyKey.java
│   ├── dto/JobCreateRequest.java
│   ├── dto/JobCreateResponse.java
│   ├── dto/JobResponse.java
│   ├── repository/JobRepository.java
│   ├── repository/IdempotencyKeyRepository.java
│   ├── service/HoldService.java
│   ├── service/HoldResult.java
│   ├── service/ConfirmService.java
│   ├── service/FailureService.java
│   ├── service/RetryService.java
│   ├── service/RefundService.java
│   ├── worker/GenerationWorker.java
│   └── stub/GenerationStubClient.java
│   └── stub/StubGenerationException.java
├── ledger/
│   ├── controller/LedgerApiController.java
│   ├── domain/LedgerEntry.java
│   ├── domain/LedgerType.java
│   ├── dto/LedgerResponse.java
│   └── repository/LedgerRepository.java
├── outbox/
│   ├── domain/OutboxEntry.java
│   ├── domain/GenerationJobMessage.java
│   ├── repository/OutboxRepository.java
│   ├── service/OutboxWriter.java
│   └── service/OutboxRelay.java
├── global/
│   ├── config/AppProperties.java
│   ├── config/PasswordEncoderConfig.java
│   ├── config/WebConfig.java
│   ├── config/KafkaTopicConfig.java
│   ├── config/DataSeeder.java
│   ├── exception/InsufficientBalanceException.java
│   ├── exception/BalanceConflictException.java
│   ├── exception/DuplicateRequestInProgressException.java
│   ├── exception/ErrorResponse.java
│   ├── exception/GlobalExceptionHandler.java
│   ├── auth/LoginInterceptor.java
│   ├── auth/SessionConst.java
│   ├── scheduler/HeartbeatRegistry.java
│   └── scheduler/DeadJobSchedulerTask.java
├── dashboard/
│   └── DashboardController.java
src/main/resources/
├── application.yml                        (application.properties 대체)
└── templates/
    ├── login.html
    └── dashboard.html
src/test/resources/application-test.yml
docker-compose.yml
```

---

### Task 1: 빌드 의존성 · 프로파일 설정 · docker-compose

**Files:**
- Modify: `build.gradle`
- Delete: `src/main/resources/application.properties`
- Create: `src/main/resources/application.yml`
- Create: `src/test/resources/application-test.yml`
- Create: `docker-compose.yml`

**Interfaces:**
- Produces: 이후 모든 태스크가 사용하는 설정 키 — `app.generation.cost`, `app.generation.max-attempts`, `app.scheduling.enabled`, `app.worker.enabled`, `app.stub.failure-rate`, `app.stub.min-delay-millis`, `app.stub.max-delay-millis`, `app.heartbeat.timeout-seconds`, `app.heartbeat.refresh-interval-seconds`, `app.kafka.topic`

- [ ] **Step 1: build.gradle 의존성 추가**

`dependencies` 블록에 아래 4줄을 추가한다 (기존 줄은 그대로 유지):

```groovy
	implementation 'org.springframework.security:spring-security-crypto'
	testImplementation 'org.springframework.boot:spring-boot-data-jpa-test'
	testImplementation 'org.springframework.boot:spring-boot-restclient'
	testImplementation 'org.springframework.boot:spring-boot-testcontainers'
	testImplementation 'org.testcontainers:testcontainers:2.0.5'
	testImplementation 'org.testcontainers:testcontainers-junit-jupiter:2.0.5'
	testImplementation 'org.testcontainers:testcontainers-mysql:2.0.5'
	testImplementation 'org.awaitility:awaitility'
```

주의:
- `spring-security-crypto`는 BCrypt만 쓰기 위한 것이다. `spring-boot-starter-security`를 추가하면 전체 인증 필터가 걸려버리므로 **절대 추가하지 말 것**.
- 이 환경의 Testcontainers는 2.x라 모듈명이 바뀌어 있다(`org.testcontainers:mysql` → `testcontainers-mysql`, `org.testcontainers:junit-jupiter` → `testcontainers-junit-jupiter`). Spring Boot의 의존성 관리가 옛 이름은 버전을 채워주지 않아 `Could not find org.testcontainers:junit-jupiter:.`처럼 버전이 빈 채로 실패한다 — 항상 위처럼 새 아티팩트명 + 명시적 버전(`2.0.5`, core `testcontainers`와 동일)으로 선언한다. 클래스 패키지 경로(`org.testcontainers.containers.MySQLContainer`, `org.testcontainers.junit.jupiter.Testcontainers`/`Container`)는 그대로이므로 이후 태스크의 import는 수정할 필요 없다.
- `@DataJpaTest`가 Spring Boot 4.1부터 `spring-boot-starter-data-jpa-test`에서 분리돼 별도 모듈 `spring-boot-data-jpa-test`로 빠졌고, 패키지도 `org.springframework.boot.test.autoconfigure.orm.jpa` → `org.springframework.boot.data.jpa.test.autoconfigure`로 바뀌었다. 이 플랜의 모든 `@DataJpaTest` import는 새 패키지 경로로 이미 통일해뒀다.
- `TestRestTemplate`도 Spring Boot 4.1부터 별도 모듈 `spring-boot-resttestclient`로 빠지고 패키지가 `org.springframework.boot.test.web.client` → `org.springframework.boot.resttestclient`로 바뀌었다. 게다가 이 모듈은 `@AutoConfigureTestRestTemplate`을 테스트 클래스에 명시해야만 `TestRestTemplate` 빈이 자동구성된다(과거처럼 `@SpringBootTest(webEnvironment=RANDOM_PORT)`만으로는 안 됨). `TestRestTemplate` 자동구성은 `RestTemplateBuilder`(`spring-boot-restclient` 모듈)가 클래스패스에 있어야 조건이 충족되므로 위 의존성이 필요하다. 또한 이 버전의 `TestRestTemplate`은 **기본적으로 리다이렉트를 따라간다**(과거와 반대) — 3xx 응답 자체를 검증하려면 `restTemplate.withRedirects(org.springframework.boot.http.client.HttpRedirects.DONT_FOLLOW)`로 리다이렉트를 끈 인스턴스를 받아 써야 한다. 이 플랜의 모든 `TestRestTemplate` 기반 테스트는 이 세 가지(새 import, `@AutoConfigureTestRestTemplate`, `withRedirects`)를 이미 반영했다.

- [ ] **Step 2: application.properties 삭제, application.yml 생성**

`src/main/resources/application.properties`를 삭제하고 `src/main/resources/application.yml` 생성:

```yaml
spring:
  application:
    name: credit_system
  datasource:
    url: jdbc:mysql://localhost:3306/credit_system
    username: credit
    password: credit
  jpa:
    hibernate:
      ddl-auto: update
    open-in-view: false
    properties:
      hibernate:
        format_sql: true
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      group-id: generation-worker
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
  data:
    redis:
      host: localhost
      port: 6379

app:
  scheduling:
    enabled: true
  worker:
    enabled: true
  seed:
    enabled: true
  kafka:
    topic: generation-jobs
  generation:
    cost: 100
    max-attempts: 3
  stub:
    failure-rate: 0.3
    min-delay-millis: 5000
    max-delay-millis: 15000
  heartbeat:
    timeout-seconds: 10
    refresh-interval-seconds: 5

logging:
  level:
    com.example.credit_system: info
```

- [ ] **Step 3: 테스트 프로파일 생성**

`src/test/resources/application-test.yml` 생성 (디렉토리 `src/test/resources`가 없으면 만든다):

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:credit_test;MODE=MySQL;DB_CLOSE_DELAY=-1
    username: sa
    password:
    driver-class-name: org.h2.Driver
  jpa:
    hibernate:
      ddl-auto: create-drop
    open-in-view: false
  kafka:
    bootstrap-servers: localhost:9092
    admin:
      fail-fast: false
    properties:
      default.api.timeout.ms: 3000
      request.timeout.ms: 3000

app:
  scheduling:
    enabled: false
  worker:
    enabled: false
  seed:
    enabled: false
  kafka:
    topic: generation-jobs
  generation:
    cost: 100
    max-attempts: 3
  stub:
    failure-rate: 0.0
    min-delay-millis: 0
    max-delay-millis: 0
  heartbeat:
    timeout-seconds: 10
    refresh-interval-seconds: 5
```

포인트:
- `MODE=MySQL`은 H2가 MySQL 문법을 흉내내게 한다.
- `app.scheduling.enabled=false`/`app.worker.enabled=false`는 테스트에서 우리 스케줄러와 Kafka 리스너가 뜨지 않게 하는 자체 스위치다 (Task 10~13에서 이 프로퍼티로 조건부 활성화를 구현한다).
- `spring.kafka.properties.*.timeout.ms`를 3초로 줄인 이유: Task 10부터 `NewTopic` 빈(`KafkaTopicConfig`)이 생기면 Spring의 `KafkaAdmin`이 컨텍스트 기동 시 브로커에 접속을 시도한다. 로컬에 docker-compose Kafka가 안 떠 있는 상태로 `./gradlew test`를 돌리면 기본 타임아웃(60초)만큼 각 테스트 클래스 기동이 느려지므로, 실제 Kafka 통신이 필요 없는 테스트(Task 9, 14, 15 등)가 빠르게 실패/스킵되도록 짧게 잡는다. Task 18의 E2E 테스트는 `@DynamicPropertySource`로 이 값을 EmbeddedKafka 브로커 주소로 덮어쓴다.

- [ ] **Step 4: docker-compose.yml 생성 (프로젝트 루트)**

```yaml
services:
  mysql:
    image: mysql:8.4
    ports:
      - "3306:3306"
    environment:
      MYSQL_DATABASE: credit_system
      MYSQL_USER: credit
      MYSQL_PASSWORD: credit
      MYSQL_ROOT_PASSWORD: root
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      retries: 10

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  kafka:
    image: bitnami/kafka:3.7
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
```

- [ ] **Step 5: 검증**

```bash
./gradlew compileJava
docker compose config --quiet
```

둘 다 에러 없이 끝나야 한다. (docker compose는 문법 검증만 — 실제 기동은 개발 실행 시점에 하면 된다.)

- [ ] **Step 6: 커밋**

```bash
git add build.gradle src/main/resources src/test/resources docker-compose.yml
git rm src/main/resources/application.properties 2>/dev/null || true
git commit -m "chore: add infra config (yml profiles, docker-compose, test deps)"
```

---

### Task 2: global 예외 · AppProperties

**Files:**
- Create: `src/main/java/com/example/credit_system/global/exception/InsufficientBalanceException.java`
- Create: `src/main/java/com/example/credit_system/global/exception/BalanceConflictException.java`
- Create: `src/main/java/com/example/credit_system/global/exception/DuplicateRequestInProgressException.java`
- Create: `src/main/java/com/example/credit_system/global/exception/ErrorResponse.java`
- Create: `src/main/java/com/example/credit_system/global/exception/GlobalExceptionHandler.java`
- Create: `src/main/java/com/example/credit_system/global/config/AppProperties.java`
- Modify: `src/main/java/com/example/credit_system/CreditSystemApplication.java`
- Test: `src/test/java/com/example/credit_system/global/exception/GlobalExceptionHandlerTest.java`

**Interfaces:**
- Produces:
  - `InsufficientBalanceException(long balance, long required)` → HTTP 409, code `INSUFFICIENT_BALANCE`
  - `BalanceConflictException(String message)` → HTTP 409, code `BALANCE_CONFLICT`
  - `DuplicateRequestInProgressException()` → HTTP 409, code `DUPLICATE_IN_PROGRESS`
  - `ErrorResponse(String code, String message)` — record
  - `AppProperties` — `@ConfigurationProperties(prefix="app")`, 하위 record: `Generation(long cost, int maxAttempts)`, `Stub(double failureRate, long minDelayMillis, long maxDelayMillis)`, `Heartbeat(long timeoutSeconds, long refreshIntervalSeconds)`, `Kafka(String topic)`

- [ ] **Step 1: 예외 클래스 3개 작성**

`InsufficientBalanceException.java`:

```java
package com.example.credit_system.global.exception;

public class InsufficientBalanceException extends RuntimeException {

    public InsufficientBalanceException(long balance, long required) {
        super("잔액이 부족합니다. balance=" + balance + ", required=" + required);
    }
}
```

`BalanceConflictException.java`:

```java
package com.example.credit_system.global.exception;

public class BalanceConflictException extends RuntimeException {

    public BalanceConflictException(String message) {
        super(message);
    }
}
```

`DuplicateRequestInProgressException.java`:

```java
package com.example.credit_system.global.exception;

public class DuplicateRequestInProgressException extends RuntimeException {

    public DuplicateRequestInProgressException() {
        super("동일한 요청이 처리 중입니다. 잠시 후 다시 시도해주세요.");
    }
}
```

- [ ] **Step 2: ErrorResponse + GlobalExceptionHandler 작성**

`ErrorResponse.java`:

```java
package com.example.credit_system.global.exception;

public record ErrorResponse(String code, String message) {
}
```

`GlobalExceptionHandler.java`:

```java
package com.example.credit_system.global.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InsufficientBalanceException.class)
    public ResponseEntity<ErrorResponse> handleInsufficientBalance(InsufficientBalanceException e) {
        return conflict("INSUFFICIENT_BALANCE", e.getMessage());
    }

    @ExceptionHandler(BalanceConflictException.class)
    public ResponseEntity<ErrorResponse> handleBalanceConflict(BalanceConflictException e) {
        return conflict("BALANCE_CONFLICT", e.getMessage());
    }

    @ExceptionHandler(DuplicateRequestInProgressException.class)
    public ResponseEntity<ErrorResponse> handleDuplicateInProgress(DuplicateRequestInProgressException e) {
        return conflict("DUPLICATE_IN_PROGRESS", e.getMessage());
    }

    private ResponseEntity<ErrorResponse> conflict(String code, String message) {
        log.info("business exception: code={}, message={}", code, message);
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(code, message));
    }
}
```

- [ ] **Step 3: AppProperties 작성 + 애플리케이션에 등록**

`AppProperties.java`:

```java
package com.example.credit_system.global.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        Generation generation,
        Stub stub,
        Heartbeat heartbeat,
        Kafka kafka
) {

    public record Generation(long cost, int maxAttempts) {
    }

    public record Stub(double failureRate, long minDelayMillis, long maxDelayMillis) {
    }

    public record Heartbeat(long timeoutSeconds, long refreshIntervalSeconds) {
    }

    public record Kafka(String topic) {
    }
}
```

`CreditSystemApplication.java`를 아래로 교체:

```java
package com.example.credit_system;

import com.example.credit_system.global.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableConfigurationProperties(AppProperties.class)
@SpringBootApplication
public class CreditSystemApplication {

    public static void main(String[] args) {
        SpringApplication.run(CreditSystemApplication.class, args);
    }
}
```

- [ ] **Step 4: 핸들러 테스트 작성 (mock 없이 — 핸들러는 순수 객체이므로 직접 호출)**

`src/test/java/com/example/credit_system/global/exception/GlobalExceptionHandlerTest.java`:

```java
package com.example.credit_system.global.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void 잔액부족_예외는_409와_코드를_반환한다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleInsufficientBalance(new InsufficientBalanceException(50, 100));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("INSUFFICIENT_BALANCE");
    }

    @Test
    void 잔액충돌_예외는_409와_코드를_반환한다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleBalanceConflict(new BalanceConflictException("conflict"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("BALANCE_CONFLICT");
    }

    @Test
    void 중복처리중_예외는_409와_코드를_반환한다() {
        ResponseEntity<ErrorResponse> response =
                handler.handleDuplicateInProgress(new DuplicateRequestInProgressException());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("DUPLICATE_IN_PROGRESS");
    }
}
```

- [ ] **Step 5: 테스트 실행**

```bash
./gradlew test --tests "com.example.credit_system.global.exception.GlobalExceptionHandlerTest"
```

Expected: 3 tests PASS

- [ ] **Step 6: 커밋**

```bash
git add src/main/java/com/example/credit_system/global src/main/java/com/example/credit_system/CreditSystemApplication.java src/test/java
git commit -m "feat: add global exceptions, error handler, app properties"
```

---

### Task 3: Organization · LedgerEntry 엔티티와 조건부 UPDATE 리포지토리

**Files:**
- Create: `src/main/java/com/example/credit_system/organization/domain/Organization.java`
- Create: `src/main/java/com/example/credit_system/organization/repository/OrganizationRepository.java`
- Create: `src/main/java/com/example/credit_system/ledger/domain/LedgerType.java`
- Create: `src/main/java/com/example/credit_system/ledger/domain/LedgerEntry.java`
- Create: `src/main/java/com/example/credit_system/ledger/repository/LedgerRepository.java`
- Test: `src/test/java/com/example/credit_system/organization/repository/OrganizationRepositoryTest.java`

**Interfaces:**
- Produces:
  - `Organization(String name, long balance)` 생성자, getter: `getId():Long`, `getName()`, `getBalance():long`, `getVersion():long`
  - `OrganizationRepository.deductBalance(Long id, long amount, long expectedVersion, Instant now): int`
  - `OrganizationRepository.addBalance(Long id, long amount, long expectedVersion, Instant now): int`
  - `LedgerEntry.of(Long organizationId, Long jobId, LedgerType type, long amount): LedgerEntry`
  - `LedgerType` enum: `HOLD, CONFIRM, REFUND, CHARGE`
  - `LedgerRepository.findByOrganizationIdOrderByIdDesc(Long organizationId): List<LedgerEntry>`

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/organization/repository/OrganizationRepositoryTest.java`:

```java
package com.example.credit_system.organization.repository;

import com.example.credit_system.organization.domain.Organization;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class OrganizationRepositoryTest {

    @Autowired
    OrganizationRepository organizationRepository;

    @Test
    void 버전이_일치하면_차감되고_버전이_증가한다() {
        Organization org = organizationRepository.save(new Organization("acme", 1000L));

        int updated = organizationRepository.deductBalance(org.getId(), 300L, 0L, Instant.now());
        organizationRepository.flush();

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getBalance()).isEqualTo(700L);
        assertThat(found.getVersion()).isEqualTo(1L);
    }

    @Test
    void 버전이_불일치하면_0행이_반환되고_잔액이_변하지_않는다() {
        Organization org = organizationRepository.save(new Organization("acme", 1000L));

        int updated = organizationRepository.deductBalance(org.getId(), 300L, 99L, Instant.now());

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isZero();
        assertThat(found.getBalance()).isEqualTo(1000L);
        assertThat(found.getVersion()).isZero();
    }

    @Test
    void 환불은_잔액을_되돌리고_버전을_증가시킨다() {
        Organization org = organizationRepository.save(new Organization("acme", 700L));

        int updated = organizationRepository.addBalance(org.getId(), 300L, 0L, Instant.now());

        Organization found = organizationRepository.findById(org.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getBalance()).isEqualTo(1000L);
        assertThat(found.getVersion()).isEqualTo(1L);
    }
}
```

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `Organization`, `OrganizationRepository` 심볼을 찾을 수 없음

- [ ] **Step 3: 엔티티/리포지토리 구현**

`Organization.java`:

```java
package com.example.credit_system.organization.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Getter
@Table(name = "organizations")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Organization {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private long balance;

    @Column(nullable = false)
    private long version;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    public Organization(String name, long balance) {
        this.name = name;
        this.balance = balance;
        this.version = 0L;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }
}
```

주의: setter가 없다. balance/version 변경은 **오직 리포지토리의 조건부 UPDATE로만** 이루어진다. 이것이 이 프로젝트의 핵심 규율이다.

`OrganizationRepository.java`:

```java
package com.example.credit_system.organization.repository;

import com.example.credit_system.organization.domain.Organization;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface OrganizationRepository extends JpaRepository<Organization, Long> {

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Organization o
            SET o.balance = o.balance - :amount, o.version = o.version + 1, o.updatedAt = :now
            WHERE o.id = :id AND o.version = :expectedVersion
            """)
    int deductBalance(@Param("id") Long id,
                      @Param("amount") long amount,
                      @Param("expectedVersion") long expectedVersion,
                      @Param("now") Instant now);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Organization o
            SET o.balance = o.balance + :amount, o.version = o.version + 1, o.updatedAt = :now
            WHERE o.id = :id AND o.version = :expectedVersion
            """)
    int addBalance(@Param("id") Long id,
                   @Param("amount") long amount,
                   @Param("expectedVersion") long expectedVersion,
                   @Param("now") Instant now);
}
```

`clearAutomatically = true`가 중요하다: `@Modifying` 쿼리는 영속성 컨텍스트(1차 캐시)를 우회해 DB에 직접 쓴다. clear를 하지 않으면 같은 트랜잭션에서 `findById`가 **UPDATE 이전의 낡은 캐시 값**을 돌려주는 버그가 생긴다.

`LedgerType.java`:

```java
package com.example.credit_system.ledger.domain;

public enum LedgerType {
    HOLD, CONFIRM, REFUND, CHARGE
}
```

`LedgerEntry.java`:

```java
package com.example.credit_system.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Getter
@Table(name = "ledger_entries")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long organizationId;

    private Long jobId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private LedgerType type;

    @Column(nullable = false)
    private long amount;

    @Column(nullable = false)
    private Instant createdAt;

    private LedgerEntry(Long organizationId, Long jobId, LedgerType type, long amount) {
        this.organizationId = organizationId;
        this.jobId = jobId;
        this.type = type;
        this.amount = amount;
        this.createdAt = Instant.now();
    }

    public static LedgerEntry of(Long organizationId, Long jobId, LedgerType type, long amount) {
        return new LedgerEntry(organizationId, jobId, type, amount);
    }
}
```

`LedgerRepository.java`:

```java
package com.example.credit_system.ledger.repository;

import com.example.credit_system.ledger.domain.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LedgerRepository extends JpaRepository<LedgerEntry, Long> {

    List<LedgerEntry> findByOrganizationIdOrderByIdDesc(Long organizationId);
}
```

ledger는 insert-only다. UPDATE/DELETE 메서드를 절대 추가하지 말 것.

- [ ] **Step 4: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.organization.repository.OrganizationRepositoryTest"
```

Expected: 3 tests PASS

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/com/example/credit_system/organization src/main/java/com/example/credit_system/ledger src/test/java/com/example/credit_system/organization
git commit -m "feat: add Organization/Ledger entities with conditional-update repository"
```

---

### Task 4: Job · IdempotencyKey · OutboxEntry 엔티티와 조건부 UPDATE 리포지토리

**Files:**
- Create: `src/main/java/com/example/credit_system/job/domain/JobStatus.java`
- Create: `src/main/java/com/example/credit_system/job/domain/Job.java`
- Create: `src/main/java/com/example/credit_system/job/domain/IdempotencyKey.java`
- Create: `src/main/java/com/example/credit_system/job/repository/JobRepository.java`
- Create: `src/main/java/com/example/credit_system/job/repository/IdempotencyKeyRepository.java`
- Create: `src/main/java/com/example/credit_system/outbox/domain/OutboxEntry.java`
- Create: `src/main/java/com/example/credit_system/outbox/repository/OutboxRepository.java`
- Test: `src/test/java/com/example/credit_system/job/repository/JobRepositoryTest.java`
- Test: `src/test/java/com/example/credit_system/job/repository/IdempotencyKeyRepositoryTest.java`

**Interfaces:**
- Produces:
  - `JobStatus` enum: `HOLDING, PROCESSING, COMPLETED, FAILED, REFUNDED`
  - `Job.hold(Long organizationId, long holdAmount, String prompt): Job` — status=HOLDING, attemptNo=0
  - Job getter: `getId():Long`, `getOrganizationId():Long`, `getStatus():JobStatus`, `getAttemptNo():int`, `getHoldAmount():long`, `getPrompt():String`, `getResultUrl():String`
  - `JobRepository.updateStatusIfAttemptMatches(Long jobId, JobStatus status, int attemptNo, Instant now): int`
  - `JobRepository.completeIfAttemptMatches(Long jobId, String resultUrl, int attemptNo, Instant now): int`
  - `JobRepository.transitionIfStatusAndAttemptMatch(Long jobId, JobStatus newStatus, JobStatus expectedStatus, int attemptNo, Instant now): int`
  - `JobRepository.incrementAttemptForRetry(Long jobId, int expectedAttemptNo, Instant now): int` — FAILED→PROCESSING, attemptNo+1
  - `JobRepository.findByStatusOrderByIdAsc(JobStatus status): List<Job>`
  - `JobRepository.findByOrganizationIdOrderByIdDesc(Long organizationId): List<Job>`
  - `IdempotencyKey(Long organizationId, String idemKey)` 생성자, getter `getJobId():Long` (mutation 메서드 없음 — 상태 변경은 리포지토리로만)
  - `IdempotencyKeyRepository.findByOrganizationIdAndIdemKey(Long organizationId, String idemKey): Optional<IdempotencyKey>` — `save`/`saveAndFlush`로 INSERT 시도, unique 제약 위반 시 `DataIntegrityViolationException`을 던짐(호출부에서 캐치해 중복 판정 — H2가 `INSERT IGNORE`를 지원하지 않아 이 방식으로 통일)
  - `IdempotencyKeyRepository.attachJobId(Long organizationId, String idemKey, Long jobId): int`
  - `OutboxEntry(Long jobId, String payload)` 생성자, getter `getId()`, `getJobId()`, `getPayload()`, `isSent()`
  - `OutboxRepository.findBySentFalseOrderByIdAsc(): List<OutboxEntry>`
  - `OutboxRepository.markSent(Long id): int`

- [ ] **Step 1: 실패하는 테스트 작성 (JobRepositoryTest)**

`src/test/java/com/example/credit_system/job/repository/JobRepositoryTest.java`:

```java
package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class JobRepositoryTest {

    @Autowired
    JobRepository jobRepository;

    @Test
    void attemptNo가_일치하면_상태가_변경된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int updated = jobRepository.updateStatusIfAttemptMatches(
                job.getId(), JobStatus.PROCESSING, 0, Instant.now());

        assertThat(updated).isEqualTo(1);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.PROCESSING);
    }

    @Test
    void attemptNo가_불일치하면_0행이며_상태가_유지된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int updated = jobRepository.updateStatusIfAttemptMatches(
                job.getId(), JobStatus.PROCESSING, 5, Instant.now());

        assertThat(updated).isZero();
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.HOLDING);
    }

    @Test
    void 완료전이는_resultUrl을_함께_기록한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int updated = jobRepository.completeIfAttemptMatches(
                job.getId(), "https://stub/image/1.png", 0, Instant.now());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(found.getResultUrl()).isEqualTo("https://stub/image/1.png");
    }

    @Test
    void 상태와_attemptNo가_모두_일치할_때만_전이된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.updateStatusIfAttemptMatches(job.getId(), JobStatus.FAILED, 0, Instant.now());

        int wrongStatus = jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.COMPLETED, 0, Instant.now());
        int match = jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.FAILED, 0, Instant.now());

        assertThat(wrongStatus).isZero();
        assertThat(match).isEqualTo(1);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.REFUNDED);
    }

    @Test
    void 재시도_투입은_FAILED_상태에서만_attemptNo를_증가시킨다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        int beforeFail = jobRepository.incrementAttemptForRetry(job.getId(), 0, Instant.now());
        assertThat(beforeFail).isZero(); // 아직 HOLDING이므로 실패

        jobRepository.updateStatusIfAttemptMatches(job.getId(), JobStatus.FAILED, 0, Instant.now());
        int afterFail = jobRepository.incrementAttemptForRetry(job.getId(), 0, Instant.now());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(afterFail).isEqualTo(1);
        assertThat(found.getStatus()).isEqualTo(JobStatus.PROCESSING);
        assertThat(found.getAttemptNo()).isEqualTo(1);
    }
}
```

- [ ] **Step 2: 실패하는 테스트 작성 (IdempotencyKeyRepositoryTest)**

`src/test/java/com/example/credit_system/job/repository/IdempotencyKeyRepositoryTest.java`:

```java
package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.IdempotencyKey;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ActiveProfiles("test")
@DataJpaTest
class IdempotencyKeyRepositoryTest {

    @Autowired
    IdempotencyKeyRepository idempotencyKeyRepository;

    @Test
    void 동일_조직_동일_키는_유니크_제약으로_거부된다() {
        idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(1L, "key-1"));

        assertThatThrownBy(() ->
                idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(1L, "key-1")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void 다른_조직은_같은_키를_사용할_수_있다() {
        idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(1L, "key-1"));
        idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(2L, "key-1"));

        assertThat(idempotencyKeyRepository.count()).isEqualTo(2);
    }

    @Test
    void attachJobId로_job을_연결할_수_있다() {
        idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(1L, "key-1"));

        int updated = idempotencyKeyRepository.attachJobId(1L, "key-1", 42L);

        IdempotencyKey found = idempotencyKeyRepository
                .findByOrganizationIdAndIdemKey(1L, "key-1").orElseThrow();
        assertThat(updated).isEqualTo(1);
        assertThat(found.getJobId()).isEqualTo(42L);
    }
}
```

- [ ] **Step 3: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `Job`, `JobStatus`, `IdempotencyKey` 등 심볼 없음

- [ ] **Step 4: 엔티티/리포지토리 구현**

`JobStatus.java`:

```java
package com.example.credit_system.job.domain;

public enum JobStatus {
    HOLDING, PROCESSING, COMPLETED, FAILED, REFUNDED
}
```

`Job.java`:

```java
package com.example.credit_system.job.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Getter
@Table(name = "jobs")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Job {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long organizationId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private JobStatus status;

    @Column(nullable = false)
    private int attemptNo;

    @Column(nullable = false)
    private long holdAmount;

    @Column(nullable = false, length = 1000)
    private String prompt;

    private String resultUrl;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    private Job(Long organizationId, long holdAmount, String prompt) {
        this.organizationId = organizationId;
        this.status = JobStatus.HOLDING;
        this.attemptNo = 0;
        this.holdAmount = holdAmount;
        this.prompt = prompt;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public static Job hold(Long organizationId, long holdAmount, String prompt) {
        return new Job(organizationId, holdAmount, prompt);
    }
}
```

상태 전이 setter가 없는 것이 의도다. 모든 전이는 조건부 UPDATE 쿼리로만 일어난다.

`IdempotencyKey.java`:

```java
package com.example.credit_system.job.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@Table(name = "idempotency_keys",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_idempotency_org_key",
                columnNames = {"organizationId", "idemKey"}))
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class IdempotencyKey {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long organizationId;

    @Column(nullable = false, length = 100)
    private String idemKey;

    private Long jobId;

    public IdempotencyKey(Long organizationId, String idemKey) {
        this.organizationId = organizationId;
        this.idemKey = idemKey;
    }
}
```

setter/mutation 메서드가 없다. `jobId` 연결은 Organization/Job과 마찬가지로 리포지토리의 조건부 UPDATE로만 한다.

`JobRepository.java`:

```java
package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface JobRepository extends JpaRepository<Job, Long> {

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.status = :status, j.updatedAt = :now
            WHERE j.id = :jobId AND j.attemptNo = :attemptNo
            """)
    int updateStatusIfAttemptMatches(@Param("jobId") Long jobId,
                                     @Param("status") JobStatus status,
                                     @Param("attemptNo") int attemptNo,
                                     @Param("now") Instant now);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.status = com.example.credit_system.job.domain.JobStatus.COMPLETED,
                j.resultUrl = :resultUrl, j.updatedAt = :now
            WHERE j.id = :jobId AND j.attemptNo = :attemptNo
            """)
    int completeIfAttemptMatches(@Param("jobId") Long jobId,
                                 @Param("resultUrl") String resultUrl,
                                 @Param("attemptNo") int attemptNo,
                                 @Param("now") Instant now);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.status = :newStatus, j.updatedAt = :now
            WHERE j.id = :jobId AND j.status = :expectedStatus AND j.attemptNo = :attemptNo
            """)
    int transitionIfStatusAndAttemptMatch(@Param("jobId") Long jobId,
                                          @Param("newStatus") JobStatus newStatus,
                                          @Param("expectedStatus") JobStatus expectedStatus,
                                          @Param("attemptNo") int attemptNo,
                                          @Param("now") Instant now);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE Job j
            SET j.attemptNo = j.attemptNo + 1,
                j.status = com.example.credit_system.job.domain.JobStatus.PROCESSING,
                j.updatedAt = :now
            WHERE j.id = :jobId
              AND j.status = com.example.credit_system.job.domain.JobStatus.FAILED
              AND j.attemptNo = :expectedAttemptNo
            """)
    int incrementAttemptForRetry(@Param("jobId") Long jobId,
                                 @Param("expectedAttemptNo") int expectedAttemptNo,
                                 @Param("now") Instant now);

    List<Job> findByStatusOrderByIdAsc(JobStatus status);

    List<Job> findByOrganizationIdOrderByIdDesc(Long organizationId);
}
```

`IdempotencyKeyRepository.java`:

```java
package com.example.credit_system.job.repository;

import com.example.credit_system.job.domain.IdempotencyKey;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, Long> {

    Optional<IdempotencyKey> findByOrganizationIdAndIdemKey(Long organizationId, String idemKey);

    @Modifying(clearAutomatically = true)
    @Query("UPDATE IdempotencyKey k SET k.jobId = :jobId WHERE k.organizationId = :organizationId AND k.idemKey = :idemKey")
    int attachJobId(@Param("organizationId") Long organizationId,
                    @Param("idemKey") String idemKey,
                    @Param("jobId") Long jobId);
}
```

중복 판정은 별도 조회용 메서드를 두지 않고, 호출부(`HoldService`, Task 9)가 `idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(...))`를 직접 시도해 unique 제약 위반 시 던져지는 `DataIntegrityViolationException`을 캐치하는 방식으로 판정한다. 처음엔 MySQL의 `INSERT IGNORE`로 예외 없이 판정하려 했으나, 테스트에 쓰는 H2가 `MODE=MySQL`에서도 `INSERT IGNORE` 문법을 지원하지 않아(`Syntax error ... expected "INTO"`) 두 DB 모두에서 동작하는 이 방식으로 정리했다.

`OutboxEntry.java`:

```java
package com.example.credit_system.outbox.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Getter
@Table(name = "outbox_entries")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long jobId;

    @Column(nullable = false, length = 2000)
    private String payload;

    @Column(nullable = false)
    private boolean sent;

    @Column(nullable = false)
    private Instant createdAt;

    public OutboxEntry(Long jobId, String payload) {
        this.jobId = jobId;
        this.payload = payload;
        this.sent = false;
        this.createdAt = Instant.now();
    }
}
```

`OutboxRepository.java`:

```java
package com.example.credit_system.outbox.repository;

import com.example.credit_system.outbox.domain.OutboxEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEntry, Long> {

    List<OutboxEntry> findBySentFalseOrderByIdAsc();

    @Modifying(clearAutomatically = true)
    @Query("UPDATE OutboxEntry o SET o.sent = true WHERE o.id = :id")
    int markSent(@Param("id") Long id);
}
```

- [ ] **Step 5: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.repository.*"
```

Expected: 8 tests PASS

- [ ] **Step 6: 커밋**

```bash
git add src/main/java/com/example/credit_system/job src/main/java/com/example/credit_system/outbox src/test/java/com/example/credit_system/job
git commit -m "feat: add Job/IdempotencyKey/Outbox entities with fencing-token queries"
```

---

### Task 5: User 엔티티

**Files:**
- Create: `src/main/java/com/example/credit_system/auth/domain/User.java`
- Create: `src/main/java/com/example/credit_system/auth/repository/UserRepository.java`
- Test: `src/test/java/com/example/credit_system/auth/repository/UserRepositoryTest.java`

**Interfaces:**
- Produces:
  - `User(Long organizationId, String username, String encodedPassword)` 생성자
  - getter: `getId():Long`, `getOrganizationId():Long`, `getUsername():String`, `getPassword():String`
  - `UserRepository.findByUsername(String username): Optional<User>`

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/auth/repository/UserRepositoryTest.java`:

```java
package com.example.credit_system.auth.repository;

import com.example.credit_system.auth.domain.User;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class UserRepositoryTest {

    @Autowired
    UserRepository userRepository;

    @Test
    void username으로_조회한다() {
        userRepository.save(new User(1L, "alice", "encoded-password"));

        User found = userRepository.findByUsername("alice").orElseThrow();

        assertThat(found.getOrganizationId()).isEqualTo(1L);
        assertThat(found.getPassword()).isEqualTo("encoded-password");
    }

    @Test
    void 없는_username은_빈_Optional을_반환한다() {
        assertThat(userRepository.findByUsername("ghost")).isEmpty();
    }
}
```

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `User`, `UserRepository` 심볼 없음

- [ ] **Step 3: 구현**

`User.java`:

```java
package com.example.credit_system.auth.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Getter
@Table(name = "users")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long organizationId;

    @Column(nullable = false, unique = true, length = 50)
    private String username;

    @Column(nullable = false)
    private String password;

    @Column(nullable = false)
    private Instant createdAt;

    public User(Long organizationId, String username, String encodedPassword) {
        this.organizationId = organizationId;
        this.username = username;
        this.password = encodedPassword;
        this.createdAt = Instant.now();
    }
}
```

`UserRepository.java`:

```java
package com.example.credit_system.auth.repository;

import com.example.credit_system.auth.domain.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface UserRepository extends JpaRepository<User, Long> {

    Optional<User> findByUsername(String username);
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.auth.repository.UserRepositoryTest"
```

Expected: 2 tests PASS

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/com/example/credit_system/auth src/test/java/com/example/credit_system/auth
git commit -m "feat: add User entity for session login"
```

---

### Task 6: 세션 로그인 (LoginController · LoginInterceptor · WebConfig)

이 태스크를 먼저 하는 이유: 이후 모든 API 컨트롤러(Task 9, 14, 15)가 `SessionConst.ORGANIZATION_ID` request attribute에 의존한다. 인증을 나중에 붙이면 컨트롤러들을 다시 손봐야 하므로 먼저 배선한다.

**Files:**
- Create: `src/main/java/com/example/credit_system/global/config/PasswordEncoderConfig.java`
- Create: `src/main/java/com/example/credit_system/global/auth/SessionConst.java`
- Create: `src/main/java/com/example/credit_system/global/auth/LoginInterceptor.java`
- Create: `src/main/java/com/example/credit_system/global/config/WebConfig.java`
- Create: `src/main/java/com/example/credit_system/auth/controller/LoginController.java`
- Test: `src/test/java/com/example/credit_system/auth/controller/LoginControllerTest.java`

**Interfaces:**
- Consumes: `UserRepository.findByUsername` (Task 5)
- Produces:
  - `SessionConst.USER_ID`, `SessionConst.ORGANIZATION_ID`, `SessionConst.USERNAME` (String 상수)
  - 로그인 성공 시 세션에 위 3개 attribute 저장, 이후 `LoginInterceptor`가 `/dashboard/**`, `/api/**` 요청마다 세션 확인 후 **request attribute**로 `ORGANIZATION_ID`/`USER_ID`를 복사한다 (컨트롤러는 `@RequestAttribute`로 꺼내 쓴다 — Task 9부터 사용)
  - 세션 없이 `/api/**` 접근 시 401, 그 외 경로는 `/login`으로 리다이렉트

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/auth/controller/LoginControllerTest.java`:

```java
package com.example.credit_system.auth.controller;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.http.client.HttpRedirects;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LoginControllerTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    UserRepository userRepository;

    @Autowired
    PasswordEncoder passwordEncoder;

    @BeforeEach
    void setUp() {
        userRepository.save(new User(1L, "alice", passwordEncoder.encode("secret123")));
    }

    @AfterEach
    void tearDown() {
        // @SpringBootTest는 @DataJpaTest와 달리 테스트 메서드마다 트랜잭션을 롤백하지 않는다
        // (실제 내장 서버 스레드가 처리하므로 롤백 경계가 다르다) — 다음 테스트의 유니크 제약 충돌을 막기 위해 직접 정리한다.
        userRepository.deleteAll();
    }

    @Test
    void 올바른_비밀번호로_로그인하면_대시보드로_리다이렉트되고_세션쿠키가_발급된다() {
        ResponseEntity<Void> response = login("alice", "secret123");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FOUND);
        // 신규 세션 생성 시 서블릿 컨테이너가 쿠키 미지원 폴백으로 ";jsessionid=..."를 경로에 붙이므로 startsWith로 확인한다.
        assertThat(response.getHeaders().getLocation().getPath()).startsWith("/dashboard");
        assertThat(response.getHeaders().get(HttpHeaders.SET_COOKIE)).isNotNull();
    }

    @Test
    void 비밀번호가_틀리면_에러_파라미터와_함께_로그인_페이지로_되돌아간다() {
        ResponseEntity<Void> response = login("alice", "wrong-password");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FOUND);
        assertThat(response.getHeaders().getLocation().getPath()).isEqualTo("/login");
        assertThat(response.getHeaders().getLocation().getQuery()).isEqualTo("error");
    }

    private ResponseEntity<Void> login(String username, String password) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", username);
        form.add("password", password);

        return restTemplate.withRedirects(HttpRedirects.DONT_FOLLOW).exchange(
                "http://localhost:" + port + "/login",
                HttpMethod.POST,
                new HttpEntity<>(form, headers),
                Void.class);
    }
}
```

주의: 이 버전의 `TestRestTemplate`은 기본적으로 리다이렉트를 따라가므로, `withRedirects(HttpRedirects.DONT_FOLLOW)`로 리다이렉트를 끈 인스턴스를 받아써야 302 응답 자체를 그대로 관찰할 수 있다.

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `LoginController`, 관련 설정 클래스 없음 (또는 아직 `/login` 라우팅이 없어 404)

- [ ] **Step 3: PasswordEncoderConfig, SessionConst 작성**

`PasswordEncoderConfig.java`:

```java
package com.example.credit_system.global.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

@Configuration
public class PasswordEncoderConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
```

`SessionConst.java`:

```java
package com.example.credit_system.global.auth;

public final class SessionConst {

    public static final String USER_ID = "USER_ID";
    public static final String ORGANIZATION_ID = "ORGANIZATION_ID";
    public static final String USERNAME = "USERNAME";

    private SessionConst() {
    }
}
```

- [ ] **Step 4: LoginInterceptor, WebConfig 작성**

`LoginInterceptor.java`:

```java
package com.example.credit_system.global.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.io.IOException;

@Component
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws IOException {
        HttpSession session = request.getSession(false);
        Object organizationId = session == null ? null : session.getAttribute(SessionConst.ORGANIZATION_ID);

        if (organizationId == null) {
            if (request.getRequestURI().startsWith("/api/")) {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            } else {
                response.sendRedirect("/login");
            }
            return false;
        }

        request.setAttribute(SessionConst.ORGANIZATION_ID, organizationId);
        request.setAttribute(SessionConst.USER_ID, session.getAttribute(SessionConst.USER_ID));
        request.setAttribute(SessionConst.USERNAME, session.getAttribute(SessionConst.USERNAME));
        return true;
    }
}
```

`WebConfig.java`:

```java
package com.example.credit_system.global.config;

import com.example.credit_system.global.auth.LoginInterceptor;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@RequiredArgsConstructor
public class WebConfig implements WebMvcConfigurer {

    private final LoginInterceptor loginInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(loginInterceptor)
                .addPathPatterns("/dashboard/**", "/api/**")
                .excludePathPatterns("/login");
    }
}
```

- [ ] **Step 5: LoginController 작성**

```java
package com.example.credit_system.auth.controller;

import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.global.auth.SessionConst;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequiredArgsConstructor
public class LoginController {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    @GetMapping("/login")
    public String loginForm() {
        return "login";
    }

    @PostMapping("/login")
    public String login(@RequestParam String username,
                         @RequestParam String password,
                         HttpServletRequest request) {
        return userRepository.findByUsername(username)
                .filter(user -> passwordEncoder.matches(password, user.getPassword()))
                .map(user -> {
                    HttpSession session = request.getSession(true);
                    session.setAttribute(SessionConst.USER_ID, user.getId());
                    session.setAttribute(SessionConst.ORGANIZATION_ID, user.getOrganizationId());
                    session.setAttribute(SessionConst.USERNAME, user.getUsername());
                    return "redirect:/dashboard";
                })
                .orElse("redirect:/login?error");
    }
}
```

`/login` GET이 반환하는 `"login"` 뷰(템플릿)는 아직 없어서 이 시점엔 GET 요청 시 500(템플릿 없음)이 나지만, 이 태스크의 테스트는 POST만 검증하므로 문제없다. 템플릿은 Task 16에서 만든다.

- [ ] **Step 6: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.auth.controller.LoginControllerTest"
```

Expected: 2 tests PASS

- [ ] **Step 7: 커밋**

```bash
git add src/main/java/com/example/credit_system/global/config/PasswordEncoderConfig.java \
        src/main/java/com/example/credit_system/global/auth \
        src/main/java/com/example/credit_system/global/config/WebConfig.java \
        src/main/java/com/example/credit_system/auth/controller \
        src/test/java/com/example/credit_system/auth/controller
git commit -m "feat: add session-based login and auth interceptor"
```

---

### Task 7: 이미지 생성 stub (GenerationStubClient)

**Files:**
- Create: `src/main/java/com/example/credit_system/job/stub/StubGenerationException.java`
- Create: `src/main/java/com/example/credit_system/job/stub/GenerationStubClient.java`
- Test: `src/test/java/com/example/credit_system/job/stub/GenerationStubClientTest.java`

**Interfaces:**
- Consumes: `AppProperties.Stub(double failureRate, long minDelayMillis, long maxDelayMillis)` (Task 2)
- Produces: `GenerationStubClient.generate(String prompt): String` — 성공 시 결과 URL 반환, 실패 시 `StubGenerationException` 던짐. mock 없이 실제 지연/확률 로직을 가진 진짜 구현체다 (design.md 6.3, 8절)

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/job/stub/GenerationStubClientTest.java`:

```java
package com.example.credit_system.job.stub;

import com.example.credit_system.global.config.AppProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationStubClientTest {

    @Test
    void failureRate가_0이면_항상_성공하고_결과_URL을_반환한다() {
        GenerationStubClient client = new GenerationStubClient(
                new AppProperties(null, new AppProperties.Stub(0.0, 0, 0), null, null));

        String resultUrl = client.generate("a cat wearing sunglasses");

        assertThat(resultUrl).startsWith("https://stub-images.local/");
    }

    @Test
    void failureRate가_1이면_항상_실패한다() {
        GenerationStubClient client = new GenerationStubClient(
                new AppProperties(null, new AppProperties.Stub(1.0, 0, 0), null, null));

        assertThatThrownBy(() -> client.generate("a cat wearing sunglasses"))
                .isInstanceOf(StubGenerationException.class);
    }
}
```

이 테스트는 Spring 컨텍스트 없이 순수 자바 객체로 동작한다 — mock이 필요 없는 이유는 `GenerationStubClient` 자체가 이미 "가짜 구현체"이기 때문이다(실제 이미지 생성 대신 지연+확률로 동작하는 진짜 코드).

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `GenerationStubClient`, `StubGenerationException` 없음

- [ ] **Step 3: 구현**

`StubGenerationException.java`:

```java
package com.example.credit_system.job.stub;

public class StubGenerationException extends RuntimeException {

    public StubGenerationException(String prompt) {
        super("이미지 생성 stub 실패: prompt=" + prompt);
    }
}
```

`GenerationStubClient.java`:

```java
package com.example.credit_system.job.stub;

import com.example.credit_system.global.config.AppProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class GenerationStubClient {

    private final AppProperties appProperties;

    public String generate(String prompt) {
        AppProperties.Stub stub = appProperties.stub();
        sleep(randomDelayMillis(stub));

        if (ThreadLocalRandom.current().nextDouble() < stub.failureRate()) {
            log.info("stub generation failed: prompt={}", prompt);
            throw new StubGenerationException(prompt);
        }

        String resultUrl = "https://stub-images.local/" + UUID.randomUUID() + ".png";
        log.info("stub generation succeeded: prompt={}, resultUrl={}", prompt, resultUrl);
        return resultUrl;
    }

    private long randomDelayMillis(AppProperties.Stub stub) {
        if (stub.maxDelayMillis() <= stub.minDelayMillis()) {
            return stub.minDelayMillis();
        }
        return ThreadLocalRandom.current().nextLong(stub.minDelayMillis(), stub.maxDelayMillis() + 1);
    }

    private void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("stub 지연 중 인터럽트 발생", e);
        }
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.stub.GenerationStubClientTest"
```

Expected: 2 tests PASS (즉시 끝남 — delay가 0이므로)

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/com/example/credit_system/job/stub src/test/java/com/example/credit_system/job/stub
git commit -m "feat: add configurable image generation stub client"
```

---

### Task 8: Outbox 기록 (GenerationJobMessage · OutboxWriter)

**Files:**
- Create: `src/main/java/com/example/credit_system/outbox/domain/GenerationJobMessage.java`
- Create: `src/main/java/com/example/credit_system/outbox/service/OutboxWriter.java`
- Test: `src/test/java/com/example/credit_system/outbox/service/OutboxWriterTest.java`

**Interfaces:**
- Consumes: `OutboxRepository` (Task 4)
- Produces:
  - `GenerationJobMessage(Long jobId, Long organizationId, int attemptNo, String prompt)` — record, outbox payload/Kafka 메시지 공통 계약
  - `OutboxWriter.write(Long jobId, Long organizationId, int attemptNo, String prompt): void` — JSON 직렬화 후 `OutboxEntry` 저장(`sent=false`)

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/outbox/service/OutboxWriterTest.java`:

```java
package com.example.credit_system.outbox.service;

import com.example.credit_system.outbox.domain.OutboxEntry;
import com.example.credit_system.outbox.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class OutboxWriterTest {

    @Autowired
    OutboxRepository outboxRepository;

    @Test
    void write하면_직렬화된_payload가_미전송_상태로_저장된다() {
        OutboxWriter outboxWriter = new OutboxWriter(outboxRepository, new ObjectMapper());

        outboxWriter.write(10L, 1L, 0, "a cat wearing sunglasses");

        List<OutboxEntry> saved = outboxRepository.findBySentFalseOrderByIdAsc();
        assertThat(saved).hasSize(1);
        assertThat(saved.get(0).getJobId()).isEqualTo(10L);
        assertThat(saved.get(0).isSent()).isFalse();
        assertThat(saved.get(0).getPayload())
                .contains("\"jobId\":10")
                .contains("\"attemptNo\":0")
                .contains("\"prompt\":\"a cat wearing sunglasses\"");
    }
}
```

`OutboxWriter`를 Spring 컨텍스트로 주입받지 않고 직접 `new`로 생성하는 이유: `@DataJpaTest`는 JPA 관련 빈만 로드하고 Jackson `ObjectMapper` 빈은 로드하지 않는다. `OutboxWriter`는 순수 생성자 주입 클래스이므로 `new ObjectMapper()`를 직접 넘겨 문제없이 테스트할 수 있다 (mock이 아니라 진짜 Jackson 객체다).

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `GenerationJobMessage`, `OutboxWriter` 없음

- [ ] **Step 3: 구현**

`GenerationJobMessage.java`:

```java
package com.example.credit_system.outbox.domain;

public record GenerationJobMessage(Long jobId, Long organizationId, int attemptNo, String prompt) {
}
```

`OutboxWriter.java`:

```java
package com.example.credit_system.outbox.service;

import com.example.credit_system.outbox.domain.GenerationJobMessage;
import com.example.credit_system.outbox.domain.OutboxEntry;
import com.example.credit_system.outbox.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OutboxWriter {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public void write(Long jobId, Long organizationId, int attemptNo, String prompt) {
        GenerationJobMessage message = new GenerationJobMessage(jobId, organizationId, attemptNo, prompt);
        outboxRepository.save(new OutboxEntry(jobId, toJson(message)));
    }

    private String toJson(GenerationJobMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("outbox payload 직렬화 실패: " + message, e);
        }
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.outbox.service.OutboxWriterTest"
```

Expected: 1 test PASS

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/com/example/credit_system/outbox src/test/java/com/example/credit_system/outbox
git commit -m "feat: add outbox writer with JSON payload contract"
```

---

### Task 9: Hold 서비스와 생성 요청 API

**Files:**
- Create: `src/main/java/com/example/credit_system/job/service/HoldResult.java`
- Create: `src/main/java/com/example/credit_system/job/service/HoldService.java`
- Create: `src/main/java/com/example/credit_system/job/dto/JobCreateRequest.java`
- Create: `src/main/java/com/example/credit_system/job/dto/JobCreateResponse.java`
- Create: `src/main/java/com/example/credit_system/job/dto/JobResponse.java`
- Create: `src/main/java/com/example/credit_system/job/controller/JobApiController.java`
- Test: `src/test/java/com/example/credit_system/job/service/HoldServiceTest.java`
- Test: `src/test/java/com/example/credit_system/job/controller/JobApiControllerTest.java`

**Interfaces:**
- Consumes: `IdempotencyKeyRepository.attachJobId/findByOrganizationIdAndIdemKey` (Task 4), `OrganizationRepository.deductBalance` (Task 3), `JobRepository`, `LedgerRepository`, `OutboxWriter.write` (Task 8), `AppProperties.generation()` (Task 2), `InsufficientBalanceException`/`BalanceConflictException`/`DuplicateRequestInProgressException` (Task 2), `SessionConst.ORGANIZATION_ID` (Task 6)
- Produces:
  - `HoldResult(Long jobId, boolean duplicate)`
  - `HoldService.requestGeneration(Long organizationId, String idemKey, String prompt): HoldResult` — design.md 6.1을 그대로 구현 (Long 버전)
  - `POST /api/jobs` body `{idemKey, prompt}` → `{jobId, duplicate}`
  - `GET /api/jobs` → `List<JobResponse>` (현재 조직의 job을 최신순으로)

- [ ] **Step 1: 실패하는 서비스 테스트 작성**

`src/test/java/com/example/credit_system/job/service/HoldServiceTest.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.exception.InsufficientBalanceException;
import com.example.credit_system.job.repository.IdempotencyKeyRepository;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.outbox.repository.OutboxRepository;
import com.example.credit_system.outbox.service.OutboxWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ActiveProfiles("test")
@DataJpaTest
class HoldServiceTest {

    @Autowired IdempotencyKeyRepository idempotencyKeyRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired OutboxRepository outboxRepository;

    HoldService holdService;
    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 1000L));
        AppProperties appProperties = new AppProperties(
                new AppProperties.Generation(100L, 3), null, null, null);
        OutboxWriter outboxWriter = new OutboxWriter(outboxRepository, new ObjectMapper());
        holdService = new HoldService(idempotencyKeyRepository, organizationRepository,
                jobRepository, ledgerRepository, outboxWriter, appProperties);
    }

    @Test
    void 정상_요청은_잔액을_차감하고_job과_ledger와_outbox를_생성한다() {
        HoldResult result = holdService.requestGeneration(organization.getId(), "key-1", "a cat");

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(result.duplicate()).isFalse();
        assertThat(found.getBalance()).isEqualTo(900L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).hasSize(1);
        assertThat(outboxRepository.findBySentFalseOrderByIdAsc()).hasSize(1);
    }

    @Test
    void 동일_idemKey로_재요청하면_같은_job을_반환하고_잔액이_추가로_차감되지_않는다() {
        HoldResult first = holdService.requestGeneration(organization.getId(), "key-1", "a cat");
        HoldResult second = holdService.requestGeneration(organization.getId(), "key-1", "a cat");

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(second.duplicate()).isTrue();
        assertThat(second.jobId()).isEqualTo(first.jobId());
        assertThat(found.getBalance()).isEqualTo(900L);
    }

    @Test
    void 잔액이_부족하면_예외가_발생하고_job이_생성되지_않는다() {
        Organization poor = organizationRepository.save(new Organization("poor", 50L));

        assertThatThrownBy(() -> holdService.requestGeneration(poor.getId(), "key-2", "a cat"))
                .isInstanceOf(InsufficientBalanceException.class);

        assertThat(jobRepository.findByOrganizationIdOrderByIdDesc(poor.getId())).isEmpty();
    }
}
```

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `HoldService`, `HoldResult` 없음

- [ ] **Step 3: HoldResult, HoldService 구현**

`HoldResult.java`:

```java
package com.example.credit_system.job.service;

public record HoldResult(Long jobId, boolean duplicate) {
}
```

`HoldService.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.global.exception.BalanceConflictException;
import com.example.credit_system.global.exception.DuplicateRequestInProgressException;
import com.example.credit_system.global.exception.InsufficientBalanceException;
import com.example.credit_system.job.domain.IdempotencyKey;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.repository.IdempotencyKeyRepository;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.outbox.service.OutboxWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class HoldService {

    private static final int MAX_LOCK_RETRIES = 3;

    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final OrganizationRepository organizationRepository;
    private final JobRepository jobRepository;
    private final LedgerRepository ledgerRepository;
    private final OutboxWriter outboxWriter;
    private final AppProperties appProperties;

    @Transactional
    public HoldResult requestGeneration(Long organizationId, String idemKey, String prompt) {
        try {
            idempotencyKeyRepository.saveAndFlush(new IdempotencyKey(organizationId, idemKey));
        } catch (DataIntegrityViolationException e) {
            return handleDuplicate(organizationId, idemKey);
        }

        long cost = appProperties.generation().cost();
        Job job = deductBalanceAndCreateJob(organizationId, cost, prompt);

        idempotencyKeyRepository.attachJobId(organizationId, idemKey, job.getId());
        ledgerRepository.save(LedgerEntry.of(organizationId, job.getId(), LedgerType.HOLD, -cost));
        outboxWriter.write(job.getId(), organizationId, job.getAttemptNo(), prompt);

        log.info("hold 완료: organizationId={}, jobId={}, cost={}", organizationId, job.getId(), cost);
        return new HoldResult(job.getId(), false);
    }

    private HoldResult handleDuplicate(Long organizationId, String idemKey) {
        IdempotencyKey existing = idempotencyKeyRepository
                .findByOrganizationIdAndIdemKey(organizationId, idemKey)
                .orElseThrow(() -> new IllegalStateException("unique 제약 위반이 감지됐는데 기존 키를 찾을 수 없음"));

        if (existing.getJobId() == null) {
            throw new DuplicateRequestInProgressException();
        }
        log.info("중복 요청 감지: organizationId={}, idemKey={}, jobId={}", organizationId, idemKey, existing.getJobId());
        return new HoldResult(existing.getJobId(), true);
    }

    private Job deductBalanceAndCreateJob(Long organizationId, long cost, String prompt) {
        for (int attempt = 0; attempt < MAX_LOCK_RETRIES; attempt++) {
            Organization organization = organizationRepository.findById(organizationId)
                    .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 organization: " + organizationId));

            if (organization.getBalance() < cost) {
                throw new InsufficientBalanceException(organization.getBalance(), cost);
            }

            int updated = organizationRepository.deductBalance(
                    organizationId, cost, organization.getVersion(), Instant.now());
            if (updated == 1) {
                return jobRepository.save(Job.hold(organizationId, cost, prompt));
            }
            log.info("잔액 차감 버전 충돌, 재시도: organizationId={}, attempt={}", organizationId, attempt);
        }
        throw new BalanceConflictException("잔액 갱신 충돌이 반복되어 요청을 처리할 수 없습니다.");
    }
}
```

- [ ] **Step 4: 서비스 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.service.HoldServiceTest"
```

Expected: 3 tests PASS

- [ ] **Step 5: DTO와 컨트롤러 작성**

`JobCreateRequest.java`:

```java
package com.example.credit_system.job.dto;

public record JobCreateRequest(String idemKey, String prompt) {
}
```

`JobCreateResponse.java`:

```java
package com.example.credit_system.job.dto;

public record JobCreateResponse(Long jobId, boolean duplicate) {
}
```

`JobResponse.java`:

```java
package com.example.credit_system.job.dto;

import com.example.credit_system.job.domain.Job;

import java.time.Instant;

public record JobResponse(Long id, String status, int attemptNo, long holdAmount,
                          String prompt, String resultUrl, Instant updatedAt) {

    public static JobResponse from(Job job) {
        return new JobResponse(job.getId(), job.getStatus().name(), job.getAttemptNo(),
                job.getHoldAmount(), job.getPrompt(), job.getResultUrl(), job.getUpdatedAt());
    }
}
```

`JobApiController.java`:

```java
package com.example.credit_system.job.controller;

import com.example.credit_system.global.auth.SessionConst;
import com.example.credit_system.global.exception.ErrorResponse;
import com.example.credit_system.job.dto.JobCreateRequest;
import com.example.credit_system.job.dto.JobCreateResponse;
import com.example.credit_system.job.dto.JobResponse;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.HoldResult;
import com.example.credit_system.job.service.HoldService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
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

    @PostMapping
    public ResponseEntity<?> create(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId,
                                     @RequestBody JobCreateRequest request) {
        if (isBlank(request.idemKey())) {
            return ResponseEntity.badRequest().body(new ErrorResponse("INVALID_REQUEST", "idemKey는 필수입니다."));
        }
        if (isBlank(request.prompt())) {
            return ResponseEntity.badRequest().body(new ErrorResponse("INVALID_REQUEST", "prompt는 필수입니다."));
        }

        HoldResult result = holdService.requestGeneration(organizationId, request.idemKey(), request.prompt());
        return ResponseEntity.ok(new JobCreateResponse(result.jobId(), result.duplicate()));
    }

    @GetMapping
    public List<JobResponse> list(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId) {
        return jobRepository.findByOrganizationIdOrderByIdDesc(organizationId).stream()
                .map(JobResponse::from)
                .toList();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
```

- [ ] **Step 6: 컨트롤러 통합 테스트 작성 (로그인 → API 호출까지 실제로 관통)**

`src/test/java/com/example/credit_system/job/controller/JobApiControllerTest.java`:

```java
package com.example.credit_system.job.controller;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.job.dto.JobCreateRequest;
import com.example.credit_system.job.dto.JobCreateResponse;
import com.example.credit_system.job.dto.JobResponse;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.http.client.HttpRedirects;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class JobApiControllerTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    UserRepository userRepository;

    @Autowired
    OrganizationRepository organizationRepository;

    @Autowired
    PasswordEncoder passwordEncoder;

    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 1000L));
        userRepository.save(new User(organization.getId(), "alice", passwordEncoder.encode("secret123")));
    }

    @AfterEach
    void tearDown() {
        userRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 세션_없이_호출하면_401이다() {
        ResponseEntity<String> response = restTemplate.getForEntity(url("/api/jobs"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void 로그인_후_생성_요청과_목록_조회가_정상_동작한다() {
        String sessionCookie = loginAndGetSessionCookie();
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, sessionCookie);
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<JobCreateResponse> createResponse = restTemplate.exchange(
                url("/api/jobs"), HttpMethod.POST,
                new HttpEntity<>(new JobCreateRequest("idem-1", "a cat"), headers),
                JobCreateResponse.class);

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(createResponse.getBody().duplicate()).isFalse();

        ResponseEntity<JobResponse[]> listResponse = restTemplate.exchange(
                url("/api/jobs"), HttpMethod.GET, new HttpEntity<>(headers), JobResponse[].class);

        List<JobResponse> jobs = List.of(listResponse.getBody());
        assertThat(jobs).hasSize(1);
        assertThat(jobs.get(0).status()).isEqualTo("HOLDING");
    }

    private String loginAndGetSessionCookie() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", "alice");
        form.add("password", "secret123");

        ResponseEntity<Void> response = restTemplate.withRedirects(HttpRedirects.DONT_FOLLOW).exchange(
                url("/login"), HttpMethod.POST, new HttpEntity<>(form, headers), Void.class);

        return response.getHeaders().get(HttpHeaders.SET_COOKIE).get(0).split(";")[0];
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
```

이 테스트가 사실상 Task 6의 `LoginInterceptor`까지 실제로 관통 검증한다 — 세션 없이는 401, 로그인 쿠키를 실어 보내면 인터셉터가 통과시켜 `organizationId`가 컨트롤러까지 전달된다.

- [ ] **Step 7: 전체 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.service.HoldServiceTest" --tests "com.example.credit_system.job.controller.JobApiControllerTest"
```

Expected: 5 tests PASS

- [ ] **Step 8: 커밋**

```bash
git add src/main/java/com/example/credit_system/job/service src/main/java/com/example/credit_system/job/dto \
        src/main/java/com/example/credit_system/job/controller src/test/java/com/example/credit_system/job
git commit -m "feat: add HoldService and job creation/list API"
```

---

### Task 10: Kafka 토픽 설정과 OutboxRelay

**Files:**
- Create: `src/main/java/com/example/credit_system/global/config/KafkaTopicConfig.java`
- Create: `src/main/java/com/example/credit_system/outbox/service/OutboxRelay.java`
- Test: `src/test/java/com/example/credit_system/outbox/service/OutboxRelayTest.java`

**Interfaces:**
- Consumes: `OutboxRepository.findBySentFalseOrderByIdAsc/markSent` (Task 4), `AppProperties.kafka().topic()` (Task 2)
- Produces: `OutboxRelay`가 `app.scheduling.enabled=true`일 때만 활성화되어 1초마다 미전송 outbox를 Kafka 토픽(`app.kafka.topic`)으로 `KafkaTemplate<String, String>.send(topic, jobId문자열, payload)` 발행 후 `markSent` 처리. payload는 `OutboxWriter`가 만든 JSON 문자열 그대로 전달한다 (Kafka 메시지 값 = outbox payload 문자열, 별도 변환 없음)

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/outbox/service/OutboxRelayTest.java`:

```java
package com.example.credit_system.outbox.service;

import com.example.credit_system.outbox.domain.OutboxEntry;
import com.example.credit_system.outbox.repository.OutboxRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.annotation.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = "generation-jobs")
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.scheduling.enabled=true"
})
class OutboxRelayTest {

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    Consumer<String, String> consumer;

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void 미전송_outbox를_카프카로_발행하고_sent를_true로_바꾼다() {
        OutboxEntry entry = outboxRepository.save(new OutboxEntry(1L, "{\"jobId\":1}"));

        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("relay-test-group", "true", embeddedKafkaBroker);
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", StringDeserializer.class);
        consumer = new KafkaConsumer<>(consumerProps);
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "generation-jobs");

        ConsumerRecord<String, String> received = KafkaTestUtils.getSingleRecord(consumer, "generation-jobs", 5000);
        assertThat(received.value()).contains("\"jobId\":1");

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(outboxRepository.findById(entry.getId()).orElseThrow().isSent()).isTrue());
    }
}
```

`app.scheduling.enabled=true`를 이 테스트에서만 다시 켜는 이유: `application-test.yml`은 기본적으로 스케줄러를 꺼두지만(다른 테스트가 의도치 않게 백그라운드에서 도는 걸 막기 위해), 이 테스트는 바로 그 스케줄러 자체를 검증해야 하므로 이 클래스에서만 켠다.

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `OutboxRelay` 없음 (`KafkaTopicConfig`도 아직 없어 `app.kafka.topic` 사용처가 없음)

- [ ] **Step 3: KafkaTopicConfig 구현**

```java
package com.example.credit_system.global.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTopicConfig {

    private final AppProperties appProperties;

    @Bean
    public NewTopic generationJobsTopic() {
        return TopicBuilder.name(appProperties.kafka().topic())
                .partitions(3)
                .replicas(1)
                .build();
    }
}
```

`@ConditionalOnProperty(..., matchIfMissing = true)`: 운영 설정(`app.worker.enabled=true`, 명시돼 있음)에선 항상 켜지고, `app.worker.enabled=false`인 테스트 프로파일에선 이 빈 자체가 생성되지 않아 컨텍스트 기동 시 Kafka 브로커에 토픽 생성을 시도하지 않는다.

- [ ] **Step 4: OutboxRelay 구현**

```java
package com.example.credit_system.outbox.service;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.outbox.domain.OutboxEntry;
import com.example.credit_system.outbox.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.scheduling", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AppProperties appProperties;

    @Transactional
    @Scheduled(fixedDelayString = "${app.scheduling.outbox-relay-interval-millis:1000}")
    public void relay() {
        List<OutboxEntry> pending = outboxRepository.findBySentFalseOrderByIdAsc();
        for (OutboxEntry entry : pending) {
            kafkaTemplate.send(appProperties.kafka().topic(), entry.getJobId().toString(), entry.getPayload());
            outboxRepository.markSent(entry.getId());
            log.info("outbox 발행: outboxId={}, jobId={}", entry.getId(), entry.getJobId());
        }
    }
}
```

Kafka 발행 성공과 `markSent` 사이에 완벽한 원자성은 없다(발행 후 프로세스가 죽으면 다음 폴링에서 같은 메시지가 다시 발행될 수 있음) — 이는 design.md 9절에 명시된 "향후 과제"이며, 워커 쪽의 attempt_no 조건부 UPDATE가 중복 소비에 대한 안전망 역할을 한다.

- [ ] **Step 5: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.outbox.service.OutboxRelayTest"
```

Expected: 1 test PASS (EmbeddedKafka 브로커를 띄우므로 몇 초 걸릴 수 있음)

- [ ] **Step 6: 커밋**

```bash
git add src/main/java/com/example/credit_system/global/config/KafkaTopicConfig.java \
        src/main/java/com/example/credit_system/outbox/service/OutboxRelay.java \
        src/test/java/com/example/credit_system/outbox/service/OutboxRelayTest.java
git commit -m "feat: add scheduled outbox relay publishing to Kafka"
```

---

### Task 11: Confirm/Failure 서비스, Redis Heartbeat, GenerationWorker

**Files:**
- Create: `src/main/java/com/example/credit_system/job/service/ConfirmService.java`
- Create: `src/main/java/com/example/credit_system/job/service/FailureService.java`
- Create: `src/main/java/com/example/credit_system/global/scheduler/HeartbeatRegistry.java`
- Create: `src/main/java/com/example/credit_system/job/worker/GenerationWorker.java`
- Test: `src/test/java/com/example/credit_system/job/service/ConfirmServiceTest.java`
- Test: `src/test/java/com/example/credit_system/job/service/FailureServiceTest.java`
- Test: `src/test/java/com/example/credit_system/job/worker/GenerationWorkerTest.java`

**Interfaces:**
- Consumes: `JobRepository.completeIfAttemptMatches/updateStatusIfAttemptMatches` (Task 4), `LedgerRepository` (Task 3), `GenerationStubClient.generate` (Task 7), `AppProperties.heartbeat()` (Task 2), `GenerationJobMessage` (Task 8)
- Produces:
  - `ConfirmService.confirm(Long jobId, int attemptNo, String resultUrl): void`
  - `FailureService.markFailed(Long jobId, int attemptNo): void`
  - `HeartbeatRegistry.startHeartbeat(Long jobId): ScheduledFuture<?>`, `stopHeartbeat(Long jobId, ScheduledFuture<?> future): void`, `findExpiredJobIds(): Set<Long>` (Task 12에서 사용)
  - `GenerationWorker` — `${app.kafka.topic}`을 구독하는 `@KafkaListener`, `app.worker.enabled=true`일 때만 활성화

- [ ] **Step 1: ConfirmService/FailureService에 대한 가벼운 실패 테스트 작성**

`src/test/java/com/example/credit_system/job/service/ConfirmServiceTest.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class ConfirmServiceTest {

    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;

    ConfirmService confirmService;

    @BeforeEach
    void setUp() {
        confirmService = new ConfirmService(jobRepository, ledgerRepository);
    }

    @Test
    void attemptNo가_일치하면_완료_처리되고_ledger가_남는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        confirmService.confirm(job.getId(), 0, "https://stub/x.png");

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(found.getResultUrl()).isEqualTo("https://stub/x.png");
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(1L)).hasSize(1);
    }

    @Test
    void attemptNo가_불일치하면_아무것도_하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        confirmService.confirm(job.getId(), 5, "https://stub/x.png");

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.HOLDING);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(1L)).isEmpty();
    }
}
```

`src/test/java/com/example/credit_system/job/service/FailureServiceTest.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class FailureServiceTest {

    @Autowired JobRepository jobRepository;

    FailureService failureService;

    @BeforeEach
    void setUp() {
        failureService = new FailureService(jobRepository);
    }

    @Test
    void attemptNo가_일치하면_FAILED로_전이한다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        failureService.markFailed(job.getId(), 0);

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    void attemptNo가_불일치하면_전이하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        failureService.markFailed(job.getId(), 9);

        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.HOLDING);
    }
}
```

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `ConfirmService`, `FailureService` 없음

- [ ] **Step 3: ConfirmService, FailureService 구현**

`ConfirmService.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConfirmService {

    private final JobRepository jobRepository;
    private final LedgerRepository ledgerRepository;

    @Transactional
    public void confirm(Long jobId, int attemptNo, String resultUrl) {
        int updated = jobRepository.completeIfAttemptMatches(jobId, resultUrl, attemptNo, Instant.now());
        if (updated == 0) {
            log.info("이미 무효화된 시도, confirm 무시: jobId={}, attemptNo={}", jobId, attemptNo);
            return;
        }
        Job job = jobRepository.findById(jobId).orElseThrow();
        ledgerRepository.save(LedgerEntry.of(job.getOrganizationId(), jobId, LedgerType.CONFIRM, 0));
        log.info("confirm 완료: jobId={}, attemptNo={}", jobId, attemptNo);
    }
}
```

`FailureService.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class FailureService {

    private final JobRepository jobRepository;

    @Transactional
    public void markFailed(Long jobId, int attemptNo) {
        int updated = jobRepository.updateStatusIfAttemptMatches(jobId, JobStatus.FAILED, attemptNo, Instant.now());
        if (updated == 0) {
            log.info("이미 무효화된 시도, 실패 처리 무시: jobId={}, attemptNo={}", jobId, attemptNo);
            return;
        }
        log.info("실패 처리: jobId={}, attemptNo={}", jobId, attemptNo);
    }
}
```

- [ ] **Step 4: Confirm/Failure 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.service.ConfirmServiceTest" --tests "com.example.credit_system.job.service.FailureServiceTest"
```

Expected: 4 tests PASS

- [ ] **Step 5: GenerationWorker에 대한 무거운 실패 테스트 작성 (Testcontainers Redis + EmbeddedKafka)**

`src/test/java/com/example/credit_system/job/worker/GenerationWorkerTest.java`:

```java
package com.example.credit_system.job.worker;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.outbox.domain.GenerationJobMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.annotation.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = "generation-jobs")
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.worker.enabled=true"
})
class GenerationWorkerTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProps(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired OrganizationRepository organizationRepository;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;

    @Test
    void 카프카_메시지를_소비하면_job이_완료되고_confirm_ledger가_남는다() throws Exception {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 100L, "a cat"));

        String payload = objectMapper.writeValueAsString(
                new GenerationJobMessage(job.getId(), organization.getId(), job.getAttemptNo(), job.getPrompt()));
        kafkaTemplate.send("generation-jobs", job.getId().toString(), payload);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Job found = jobRepository.findById(job.getId()).orElseThrow();
            assertThat(found.getStatus()).isEqualTo(JobStatus.COMPLETED);
            assertThat(found.getResultUrl()).isNotNull();
        });

        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .anyMatch(entry -> entry.getType().name().equals("CONFIRM"));
    }
}
```

이 테스트는 `app.stub.failure-rate=0.0`(테스트 프로파일 기본값)에 의존해 결정론적으로 성공 경로만 검증한다. 실패/재시도 경로는 Task 13에서 별도로 검증한다.

- [ ] **Step 6: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `HeartbeatRegistry`, `GenerationWorker` 없음

- [ ] **Step 7: HeartbeatRegistry, GenerationWorker 구현**

`HeartbeatRegistry.java`:

```java
package com.example.credit_system.global.scheduler;

import com.example.credit_system.global.config.AppProperties;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class HeartbeatRegistry {

    private static final String KEY = "heartbeats";

    private final StringRedisTemplate redisTemplate;
    private final AppProperties appProperties;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    public ScheduledFuture<?> startHeartbeat(Long jobId) {
        touch(jobId);
        long interval = appProperties.heartbeat().refreshIntervalSeconds();
        return executor.scheduleAtFixedRate(() -> touch(jobId), interval, interval, TimeUnit.SECONDS);
    }

    public void stopHeartbeat(Long jobId, ScheduledFuture<?> future) {
        future.cancel(false);
        remove(jobId);
    }

    public Set<Long> findExpiredJobIds() {
        double now = Instant.now().getEpochSecond();
        Set<String> expired = redisTemplate.opsForZSet().rangeByScore(KEY, Double.NEGATIVE_INFINITY, now);
        if (expired == null || expired.isEmpty()) {
            return Set.of();
        }
        return expired.stream().map(Long::parseLong).collect(Collectors.toSet());
    }

    private void touch(Long jobId) {
        double expireAt = Instant.now().getEpochSecond() + appProperties.heartbeat().timeoutSeconds();
        redisTemplate.opsForZSet().add(KEY, jobId.toString(), expireAt);
    }

    public void remove(Long jobId) {
        redisTemplate.opsForZSet().remove(KEY, jobId.toString());
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
```

`GenerationWorker.java`:

```java
package com.example.credit_system.job.worker;

import com.example.credit_system.global.scheduler.HeartbeatRegistry;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.ConfirmService;
import com.example.credit_system.job.service.FailureService;
import com.example.credit_system.job.stub.GenerationStubClient;
import com.example.credit_system.job.stub.StubGenerationException;
import com.example.credit_system.outbox.domain.GenerationJobMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class GenerationWorker {

    private final JobRepository jobRepository;
    private final HeartbeatRegistry heartbeatRegistry;
    private final GenerationStubClient stubClient;
    private final ConfirmService confirmService;
    private final FailureService failureService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.topic}")
    public void consume(String payload) throws Exception {
        GenerationJobMessage message = objectMapper.readValue(payload, GenerationJobMessage.class);

        int updated = jobRepository.updateStatusIfAttemptMatches(
                message.jobId(), JobStatus.PROCESSING, message.attemptNo(), Instant.now());
        if (updated == 0) {
            log.info("무효한 메시지 무시: jobId={}, attemptNo={}", message.jobId(), message.attemptNo());
            return;
        }

        ScheduledFuture<?> heartbeatFuture = heartbeatRegistry.startHeartbeat(message.jobId());
        try {
            String resultUrl = stubClient.generate(message.prompt());
            confirmService.confirm(message.jobId(), message.attemptNo(), resultUrl);
        } catch (StubGenerationException e) {
            failureService.markFailed(message.jobId(), message.attemptNo());
        } finally {
            heartbeatRegistry.stopHeartbeat(message.jobId(), heartbeatFuture);
        }
    }
}
```

`updateStatusIfAttemptMatches`가 현재 status와 무관하게 attemptNo만 확인하는 것이 의도다 (design.md 4.2 워커 의사코드와 동일 — HOLDING이든 뭐든 attemptNo만 맞으면 PROCESSING으로 전이).

- [ ] **Step 8: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.worker.GenerationWorkerTest"
```

Expected: 1 test PASS (Testcontainers Redis + EmbeddedKafka 기동으로 10~20초 정도 걸릴 수 있음)

- [ ] **Step 9: 커밋**

```bash
git add src/main/java/com/example/credit_system/job/service/ConfirmService.java \
        src/main/java/com/example/credit_system/job/service/FailureService.java \
        src/main/java/com/example/credit_system/global/scheduler/HeartbeatRegistry.java \
        src/main/java/com/example/credit_system/job/worker/GenerationWorker.java \
        src/test/java/com/example/credit_system/job
git commit -m "feat: add Redis heartbeat and Kafka generation worker"
```

---

### Task 12: RetryService · RefundService · DeadJobSchedulerTask

**Files:**
- Create: `src/main/java/com/example/credit_system/job/service/RetryService.java`
- Create: `src/main/java/com/example/credit_system/job/service/RefundService.java`
- Create: `src/main/java/com/example/credit_system/global/scheduler/DeadJobSchedulerTask.java`
- Test: `src/test/java/com/example/credit_system/job/service/RetryServiceTest.java`
- Test: `src/test/java/com/example/credit_system/job/service/RefundServiceTest.java`

**Interfaces:**
- Consumes: `JobRepository.incrementAttemptForRetry/transitionIfStatusAndAttemptMatch/findByStatusOrderByIdAsc` (Task 4), `OrganizationRepository.addBalance` (Task 3), `OutboxWriter.write` (Task 8), `HeartbeatRegistry.findExpiredJobIds/remove` (Task 11), `AppProperties.generation().maxAttempts()` (Task 2)
- Produces:
  - `RetryService.retry(Job job): void` — FAILED 상태일 때만 attemptNo 증가 + outbox 재발행
  - `RefundService.finalRefund(Job job): void` — FAILED 상태일 때만 REFUNDED 전이 + 잔액 복구 + ledger REFUND
  - `DeadJobSchedulerTask` — `app.scheduling.enabled=true`일 때만 5초마다 heartbeat 만료/FAILED job을 스캔해 재시도 또는 최종 환불로 분기 (design.md 4.4)

이 태스크의 재시도/최종환불 **엔드투엔드 동작**(3회 실패 → 최종 REFUNDED)은 Task 17의 `RetryRefundTest`(Testcontainers MySQL)에서 검증한다. 여기서는 `RetryService`/`RefundService` 각각의 조건부 분기 로직만 가볍게(H2) 검증한다.

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/job/service/RetryServiceTest.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.outbox.repository.OutboxRepository;
import com.example.credit_system.outbox.service.OutboxWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class RetryServiceTest {

    @Autowired JobRepository jobRepository;
    @Autowired OutboxRepository outboxRepository;

    RetryService retryService;

    @BeforeEach
    void setUp() {
        OutboxWriter outboxWriter = new OutboxWriter(outboxRepository, new ObjectMapper());
        retryService = new RetryService(jobRepository, outboxWriter);
    }

    @Test
    void FAILED_상태의_job은_attemptNo가_증가하고_outbox가_재발행된다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));
        jobRepository.updateStatusIfAttemptMatches(job.getId(), JobStatus.FAILED, 0, Instant.now());

        retryService.retry(jobRepository.findById(job.getId()).orElseThrow());

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getStatus()).isEqualTo(JobStatus.PROCESSING);
        assertThat(found.getAttemptNo()).isEqualTo(1);
        assertThat(outboxRepository.findBySentFalseOrderByIdAsc()).hasSize(1);
    }

    @Test
    void FAILED_상태가_아니면_아무것도_하지_않는다() {
        Job job = jobRepository.save(Job.hold(1L, 100L, "cat"));

        retryService.retry(job);

        Job found = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(found.getAttemptNo()).isZero();
        assertThat(outboxRepository.findBySentFalseOrderByIdAsc()).isEmpty();
    }
}
```

`src/test/java/com/example/credit_system/job/service/RefundServiceTest.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class RefundServiceTest {

    @Autowired JobRepository jobRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    RefundService refundService;

    @BeforeEach
    void setUp() {
        refundService = new RefundService(jobRepository, organizationRepository, ledgerRepository);
    }

    @Test
    void FAILED_job은_REFUNDED로_전이되고_잔액이_복구된다() {
        Organization organization = organizationRepository.save(new Organization("acme", 700L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 300L, "cat"));
        jobRepository.updateStatusIfAttemptMatches(job.getId(), JobStatus.FAILED, 0, Instant.now());

        refundService.finalRefund(jobRepository.findById(job.getId()).orElseThrow());

        Job foundJob = jobRepository.findById(job.getId()).orElseThrow();
        Organization foundOrg = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(foundJob.getStatus()).isEqualTo(JobStatus.REFUNDED);
        assertThat(foundOrg.getBalance()).isEqualTo(1000L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .anyMatch(entry -> entry.getType().name().equals("REFUND"));
    }

    @Test
    void FAILED_상태가_아니면_환불하지_않는다() {
        Organization organization = organizationRepository.save(new Organization("acme", 700L));
        Job job = jobRepository.save(Job.hold(organization.getId(), 300L, "cat"));

        refundService.finalRefund(job);

        Organization foundOrg = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(foundOrg.getBalance()).isEqualTo(700L);
    }
}
```

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `RetryService`, `RefundService` 없음

- [ ] **Step 3: RetryService, RefundService 구현**

`RetryService.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.outbox.service.OutboxWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class RetryService {

    private final JobRepository jobRepository;
    private final OutboxWriter outboxWriter;

    @Transactional
    public void retry(Job job) {
        int updated = jobRepository.incrementAttemptForRetry(job.getId(), job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("재시도 투입 경쟁에서 밀림 또는 이미 처리됨: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return;
        }
        int newAttemptNo = job.getAttemptNo() + 1;
        outboxWriter.write(job.getId(), job.getOrganizationId(), newAttemptNo, job.getPrompt());
        log.info("재시도 투입: jobId={}, newAttemptNo={}", job.getId(), newAttemptNo);
    }
}
```

`RefundService.java`:

```java
package com.example.credit_system.job.service;

import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class RefundService {

    private static final int MAX_LOCK_RETRIES = 3;

    private final JobRepository jobRepository;
    private final OrganizationRepository organizationRepository;
    private final LedgerRepository ledgerRepository;

    @Transactional
    public void finalRefund(Job job) {
        int updated = jobRepository.transitionIfStatusAndAttemptMatch(
                job.getId(), JobStatus.REFUNDED, JobStatus.FAILED, job.getAttemptNo(), Instant.now());
        if (updated == 0) {
            log.info("이미 늦은 워커가 처리함, 환불 취소: jobId={}, attemptNo={}", job.getId(), job.getAttemptNo());
            return;
        }

        for (int attempt = 0; attempt < MAX_LOCK_RETRIES; attempt++) {
            Organization organization = organizationRepository.findById(job.getOrganizationId())
                    .orElseThrow(() -> new IllegalStateException("존재하지 않는 organization: " + job.getOrganizationId()));

            int orgUpdated = organizationRepository.addBalance(
                    job.getOrganizationId(), job.getHoldAmount(), organization.getVersion(), Instant.now());
            if (orgUpdated == 1) {
                ledgerRepository.save(LedgerEntry.of(job.getOrganizationId(), job.getId(), LedgerType.REFUND, job.getHoldAmount()));
                log.info("최종 환불 완료: jobId={}, organizationId={}, amount={}",
                        job.getId(), job.getOrganizationId(), job.getHoldAmount());
                return;
            }
            log.info("환불 중 잔액 버전 충돌, 재시도: organizationId={}, attempt={}", job.getOrganizationId(), attempt);
        }
        log.error("환불 잔액 반영이 반복 충돌로 실패함 - 운영 알림 필요: jobId={}, organizationId={}, amount={}",
                job.getId(), job.getOrganizationId(), job.getHoldAmount());
    }
}
```

`finalRefund`가 잔액 반영에 3회 모두 실패해도 job.status는 이미 REFUNDED로 남는다 — design.md 9절에 명시된 미해결 과제(운영 알림 정책 미정)를 그대로 반영한 것이며, 이 시점엔 ERROR 로그만 남긴다.

- [ ] **Step 4: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.service.RetryServiceTest" --tests "com.example.credit_system.job.service.RefundServiceTest"
```

Expected: 4 tests PASS

- [ ] **Step 5: DeadJobSchedulerTask 구현 (테스트 없이 — Task 17에서 통합 검증)**

`src/main/java/com/example/credit_system/global/scheduler/DeadJobSchedulerTask.java`:

```java
package com.example.credit_system.global.scheduler;

import com.example.credit_system.global.config.AppProperties;
import com.example.credit_system.job.domain.Job;
import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.RefundService;
import com.example.credit_system.job.service.RetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.scheduling", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DeadJobSchedulerTask {

    private final HeartbeatRegistry heartbeatRegistry;
    private final JobRepository jobRepository;
    private final RetryService retryService;
    private final RefundService refundService;
    private final AppProperties appProperties;

    @Scheduled(fixedDelayString = "${app.scheduling.dead-job-scan-interval-millis:5000}")
    public void scan() {
        for (Long jobId : heartbeatRegistry.findExpiredJobIds()) {
            markExpiredAsFailed(jobId);
        }
        for (Job job : jobRepository.findByStatusOrderByIdAsc(JobStatus.FAILED)) {
            process(job);
        }
    }

    private void markExpiredAsFailed(Long jobId) {
        jobRepository.findById(jobId).ifPresent(job -> {
            int updated = jobRepository.updateStatusIfAttemptMatches(
                    jobId, JobStatus.FAILED, job.getAttemptNo(), Instant.now());
            if (updated == 1) {
                heartbeatRegistry.remove(jobId);
                log.info("heartbeat 만료로 FAILED 전이: jobId={}, attemptNo={}", jobId, job.getAttemptNo());
            }
        });
    }

    private void process(Job job) {
        // scan() 시작 시점의 스냅샷일 수 있으므로 재조회해 최신 상태로 분기한다.
        Job current = jobRepository.findById(job.getId()).orElse(null);
        if (current == null || current.getStatus() != JobStatus.FAILED) {
            return;
        }
        if (current.getAttemptNo() < appProperties.generation().maxAttempts()) {
            retryService.retry(current);
        } else {
            refundService.finalRefund(current);
        }
    }
}
```

이 컴포넌트는 Redis(heartbeat)와 스케줄링에 의존해 단위 테스트로 가볍게 검증하기 어렵다 — 실제 재시도(최대 3회)→최종 REFUNDED 흐름은 Task 17의 `RetryRefundTest`가 실제 스케줄러를 켠 채로 end-to-end 검증한다.

- [ ] **Step 6: 컴파일 확인**

```bash
./gradlew compileJava
```

Expected: SUCCESS

- [ ] **Step 7: 커밋**

```bash
git add src/main/java/com/example/credit_system/job/service/RetryService.java \
        src/main/java/com/example/credit_system/job/service/RefundService.java \
        src/main/java/com/example/credit_system/global/scheduler/DeadJobSchedulerTask.java \
        src/test/java/com/example/credit_system/job/service/RetryServiceTest.java \
        src/test/java/com/example/credit_system/job/service/RefundServiceTest.java
git commit -m "feat: add retry/refund services and dead-job scheduler"
```

---

### Task 13: 충전(Charge) 서비스와 조직 API

**Files:**
- Create: `src/main/java/com/example/credit_system/organization/service/ChargeService.java`
- Create: `src/main/java/com/example/credit_system/organization/dto/BalanceResponse.java`
- Create: `src/main/java/com/example/credit_system/organization/dto/ChargeRequest.java`
- Create: `src/main/java/com/example/credit_system/organization/controller/OrganizationApiController.java`
- Test: `src/test/java/com/example/credit_system/organization/service/ChargeServiceTest.java`
- Test: `src/test/java/com/example/credit_system/organization/controller/OrganizationApiControllerTest.java`

**Interfaces:**
- Consumes: `OrganizationRepository.addBalance` (Task 3), `LedgerRepository` (Task 3), `SessionConst.ORGANIZATION_ID` (Task 6)
- Produces:
  - `ChargeService.charge(Long organizationId, long amount): void`
  - `GET /api/organizations/me/balance` → `{balance}`
  - `POST /api/organizations/me/charge` body `{amount}` → `{balance}` (충전 후 잔액)

**스펙 대비 변경 사항**: 스펙 7절은 `POST /api/organizations/{id}/charge`(path variable)로 적었지만, 그러면 로그인한 사용자가 자기 조직이 아닌 임의의 `{id}`를 넣어 남의 조직을 충전/조회할 수 있는 구멍이 생긴다. 항상 세션의 `organizationId`만 사용하는 `/me/balance`, `/me/charge`로 구현한다.

- [ ] **Step 1: 실패하는 서비스 테스트 작성**

`src/test/java/com/example/credit_system/organization/service/ChargeServiceTest.java`:

```java
package com.example.credit_system.organization.service;

import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@DataJpaTest
class ChargeServiceTest {

    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    ChargeService chargeService;

    @BeforeEach
    void setUp() {
        chargeService = new ChargeService(organizationRepository, ledgerRepository);
    }

    @Test
    void 충전하면_잔액이_증가하고_ledger에_CHARGE가_남는다() {
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        chargeService.charge(organization.getId(), 300L);

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(800L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .anyMatch(entry -> entry.getType().name().equals("CHARGE"));
    }
}
```

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `ChargeService` 없음

- [ ] **Step 3: ChargeService 구현**

```java
package com.example.credit_system.organization.service;

import com.example.credit_system.global.exception.BalanceConflictException;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChargeService {

    private static final int MAX_LOCK_RETRIES = 3;

    private final OrganizationRepository organizationRepository;
    private final LedgerRepository ledgerRepository;

    @Transactional
    public void charge(Long organizationId, long amount) {
        for (int attempt = 0; attempt < MAX_LOCK_RETRIES; attempt++) {
            Organization organization = organizationRepository.findById(organizationId)
                    .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 organization: " + organizationId));

            int updated = organizationRepository.addBalance(
                    organizationId, amount, organization.getVersion(), Instant.now());
            if (updated == 1) {
                ledgerRepository.save(LedgerEntry.of(organizationId, null, LedgerType.CHARGE, amount));
                log.info("충전 완료: organizationId={}, amount={}", organizationId, amount);
                return;
            }
            log.info("충전 중 버전 충돌, 재시도: organizationId={}, attempt={}", organizationId, attempt);
        }
        throw new BalanceConflictException("충전 처리 중 버전 충돌이 반복되었습니다.");
    }
}
```

- [ ] **Step 4: 서비스 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.organization.service.ChargeServiceTest"
```

Expected: 1 test PASS

- [ ] **Step 5: DTO와 컨트롤러 작성**

`BalanceResponse.java`:

```java
package com.example.credit_system.organization.dto;

public record BalanceResponse(long balance) {
}
```

`ChargeRequest.java`:

```java
package com.example.credit_system.organization.dto;

public record ChargeRequest(long amount) {
}
```

`OrganizationApiController.java`:

```java
package com.example.credit_system.organization.controller;

import com.example.credit_system.global.auth.SessionConst;
import com.example.credit_system.global.exception.ErrorResponse;
import com.example.credit_system.organization.dto.BalanceResponse;
import com.example.credit_system.organization.dto.ChargeRequest;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import com.example.credit_system.organization.service.ChargeService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
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

    @GetMapping("/me/balance")
    public BalanceResponse myBalance(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId) {
        Organization organization = organizationRepository.findById(organizationId).orElseThrow();
        return new BalanceResponse(organization.getBalance());
    }

    @PostMapping("/me/charge")
    public ResponseEntity<?> charge(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId,
                                     @RequestBody ChargeRequest request) {
        if (request.amount() <= 0) {
            return ResponseEntity.badRequest().body(new ErrorResponse("INVALID_REQUEST", "amount는 0보다 커야 합니다."));
        }
        chargeService.charge(organizationId, request.amount());
        Organization organization = organizationRepository.findById(organizationId).orElseThrow();
        return ResponseEntity.ok(new BalanceResponse(organization.getBalance()));
    }
}
```

- [ ] **Step 6: 컨트롤러 통합 테스트 작성**

`src/test/java/com/example/credit_system/organization/controller/OrganizationApiControllerTest.java`:

```java
package com.example.credit_system.organization.controller;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.dto.BalanceResponse;
import com.example.credit_system.organization.dto.ChargeRequest;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.http.client.HttpRedirects;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrganizationApiControllerTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired UserRepository userRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired PasswordEncoder passwordEncoder;

    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 500L));
        userRepository.save(new User(organization.getId(), "alice", passwordEncoder.encode("secret123")));
    }

    @AfterEach
    void tearDown() {
        userRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 잔액_조회와_충전이_정상_동작한다() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, loginAndGetSessionCookie());

        ResponseEntity<BalanceResponse> before = restTemplate.exchange(
                url("/api/organizations/me/balance"), HttpMethod.GET, new HttpEntity<>(headers), BalanceResponse.class);
        assertThat(before.getBody().balance()).isEqualTo(500L);

        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<BalanceResponse> after = restTemplate.exchange(
                url("/api/organizations/me/charge"), HttpMethod.POST,
                new HttpEntity<>(new ChargeRequest(300L), headers), BalanceResponse.class);

        assertThat(after.getBody().balance()).isEqualTo(800L);
    }

    private String loginAndGetSessionCookie() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", "alice");
        form.add("password", "secret123");

        ResponseEntity<Void> response = restTemplate.withRedirects(HttpRedirects.DONT_FOLLOW).exchange(
                url("/login"), HttpMethod.POST, new HttpEntity<>(form, headers), Void.class);

        return response.getHeaders().get(HttpHeaders.SET_COOKIE).get(0).split(";")[0];
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
```

- [ ] **Step 7: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.organization.controller.OrganizationApiControllerTest"
```

Expected: 1 test PASS

- [ ] **Step 8: 커밋**

```bash
git add src/main/java/com/example/credit_system/organization src/test/java/com/example/credit_system/organization
git commit -m "feat: add charge service and organization balance API"
```

---

### Task 14: Ledger 조회 API

**Files:**
- Create: `src/main/java/com/example/credit_system/ledger/dto/LedgerResponse.java`
- Create: `src/main/java/com/example/credit_system/ledger/controller/LedgerApiController.java`
- Test: `src/test/java/com/example/credit_system/ledger/controller/LedgerApiControllerTest.java`

**Interfaces:**
- Consumes: `LedgerRepository.findByOrganizationIdOrderByIdDesc` (Task 3), `SessionConst.ORGANIZATION_ID` (Task 6)
- Produces: `GET /api/ledger` → 현재 조직의 `List<LedgerResponse>` (최신순)

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/ledger/controller/LedgerApiControllerTest.java`:

```java
package com.example.credit_system.ledger.controller;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.ledger.domain.LedgerEntry;
import com.example.credit_system.ledger.domain.LedgerType;
import com.example.credit_system.ledger.dto.LedgerResponse;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.http.client.HttpRedirects;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LedgerApiControllerTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired UserRepository userRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired PasswordEncoder passwordEncoder;

    Organization organization;

    @BeforeEach
    void setUp() {
        organization = organizationRepository.save(new Organization("acme", 1000L));
        userRepository.save(new User(organization.getId(), "alice", passwordEncoder.encode("secret123")));
        ledgerRepository.save(LedgerEntry.of(organization.getId(), 1L, LedgerType.HOLD, -100L));
        ledgerRepository.save(LedgerEntry.of(organization.getId(), null, LedgerType.CHARGE, 500L));
    }

    @AfterEach
    void tearDown() {
        ledgerRepository.deleteAll();
        userRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 세션이_있으면_ledger_내역을_최신순으로_돌려준다() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, loginAndGetSessionCookie());

        ResponseEntity<LedgerResponse[]> response = restTemplate.exchange(
                url("/api/ledger"), HttpMethod.GET, new HttpEntity<>(headers), LedgerResponse[].class);

        List<LedgerResponse> entries = List.of(response.getBody());
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).type()).isEqualTo("CHARGE");
        assertThat(entries.get(1).type()).isEqualTo("HOLD");
    }

    @Test
    void 세션_없이_호출하면_401이다() {
        ResponseEntity<String> response = restTemplate.getForEntity(url("/api/ledger"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    private String loginAndGetSessionCookie() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", "alice");
        form.add("password", "secret123");

        ResponseEntity<Void> response = restTemplate.withRedirects(HttpRedirects.DONT_FOLLOW).exchange(
                url("/login"), HttpMethod.POST, new HttpEntity<>(form, headers), Void.class);

        return response.getHeaders().get(HttpHeaders.SET_COOKIE).get(0).split(";")[0];
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
```

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `LedgerResponse`, `LedgerApiController` 없음

- [ ] **Step 3: 구현**

`LedgerResponse.java`:

```java
package com.example.credit_system.ledger.dto;

import com.example.credit_system.ledger.domain.LedgerEntry;

import java.time.Instant;

public record LedgerResponse(Long id, String type, long amount, Long jobId, Instant createdAt) {

    public static LedgerResponse from(LedgerEntry entry) {
        return new LedgerResponse(entry.getId(), entry.getType().name(), entry.getAmount(),
                entry.getJobId(), entry.getCreatedAt());
    }
}
```

`LedgerApiController.java`:

```java
package com.example.credit_system.ledger.controller;

import com.example.credit_system.global.auth.SessionConst;
import com.example.credit_system.ledger.dto.LedgerResponse;
import com.example.credit_system.ledger.repository.LedgerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/ledger")
public class LedgerApiController {

    private final LedgerRepository ledgerRepository;

    @GetMapping
    public List<LedgerResponse> list(@RequestAttribute(SessionConst.ORGANIZATION_ID) Long organizationId) {
        return ledgerRepository.findByOrganizationIdOrderByIdDesc(organizationId).stream()
                .map(LedgerResponse::from)
                .toList();
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.ledger.controller.LedgerApiControllerTest"
```

Expected: 2 tests PASS

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/com/example/credit_system/ledger/dto src/main/java/com/example/credit_system/ledger/controller \
        src/test/java/com/example/credit_system/ledger
git commit -m "feat: add ledger listing API"
```

---

### Task 15: 샘플 데이터 시더 (DataSeeder)

이 태스크가 있어야 Task 16에서 만든 대시보드를 `docker-compose` + `./gradlew bootRun`으로 실제 띄웠을 때 로그인할 계정이 존재한다.

**Files:**
- Create: `src/main/java/com/example/credit_system/global/config/DataSeeder.java`
- Test: `src/test/java/com/example/credit_system/global/config/DataSeederTest.java`

**Interfaces:**
- Consumes: `OrganizationRepository`, `UserRepository`, `PasswordEncoder`
- Produces: 애플리케이션 최초 기동 시(`app.seed.enabled=true`, 운영 기본값) organization이 하나도 없으면 샘플 조직 2개(`Acme Corp` 10,000 / `Globex Inc` 5,000)와 각 조직의 사용자(`alice`/`bob`, 비밀번호 `password123`)를 생성

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/global/config/DataSeederTest.java`:

```java
package com.example.credit_system.global.config;

import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest(properties = "app.seed.enabled=true")
class DataSeederTest {

    @Autowired OrganizationRepository organizationRepository;
    @Autowired UserRepository userRepository;

    @Test
    void 기동_시_샘플_조직과_사용자가_생성된다() {
        assertThat(organizationRepository.count()).isGreaterThanOrEqualTo(2);
        assertThat(userRepository.findByUsername("alice")).isPresent();
        assertThat(userRepository.findByUsername("bob")).isPresent();
    }
}
```

`app.seed.enabled=true`를 이 테스트에서만 켜는 이유: `application-test.yml`은 다른 테스트가 시드 데이터로 오염되지 않도록 기본값을 꺼둔다(`app.scheduling`/`app.worker`와 동일한 패턴).

- [ ] **Step 2: 컴파일 실패 확인**

```bash
./gradlew compileTestJava
```

Expected: FAIL — `DataSeeder` 없음

- [ ] **Step 3: 구현**

```java
package com.example.credit_system.global.config;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.seed", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DataSeeder implements CommandLineRunner {

    private final OrganizationRepository organizationRepository;
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    @Override
    public void run(String... args) {
        if (organizationRepository.count() > 0) {
            log.info("이미 시드 데이터가 존재해 스킵함");
            return;
        }

        Organization acme = organizationRepository.save(new Organization("Acme Corp", 10_000L));
        userRepository.save(new User(acme.getId(), "alice", passwordEncoder.encode("password123")));

        Organization globex = organizationRepository.save(new Organization("Globex Inc", 5_000L));
        userRepository.save(new User(globex.getId(), "bob", passwordEncoder.encode("password123")));

        log.info("샘플 데이터 시드 완료: alice/Acme Corp(10000), bob/Globex Inc(5000)");
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.global.config.DataSeederTest"
```

Expected: 1 test PASS

- [ ] **Step 5: 커밋**

```bash
git add src/main/java/com/example/credit_system/global/config/DataSeeder.java \
        src/test/java/com/example/credit_system/global/config/DataSeederTest.java
git commit -m "feat: add sample data seeder for local demo"
```

---

### Task 16: 대시보드 화면 (Thymeleaf + 폴링 JS)

**Files:**
- Create: `src/main/java/com/example/credit_system/dashboard/DashboardController.java`
- Create: `src/main/resources/templates/login.html`
- Create: `src/main/resources/templates/dashboard.html`
- Test: `src/test/java/com/example/credit_system/dashboard/DashboardControllerTest.java`

**Interfaces:**
- Consumes: `SessionConst.USERNAME` (Task 6, request attribute), `/api/organizations/me/balance`·`/api/jobs`·`/api/ledger`·`/api/organizations/me/charge` (Task 9, 13, 14) — 전부 브라우저 JS의 `fetch`로 호출
- Produces: `GET /login`(폼), `GET /dashboard`(잔액/충전/생성요청/job목록/ledger탭 셸 — 실제 데이터는 페이지 로드 시 JS가 `fetch`로 채우고 3초마다 폴링)

이 태스크에서 서버는 로그인 여부에 따른 셸(사용자명 표시)만 렌더링하고, 잔액/장부/작업 목록은 모두 클라이언트 JS가 이미 존재하는 REST API를 호출해 채운다 — 서버 렌더링과 JS 렌더링 두 갈래를 유지하지 않기 위한 의도적 단순화다.

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/example/credit_system/dashboard/DashboardControllerTest.java`:

```java
package com.example.credit_system.dashboard;

import com.example.credit_system.auth.domain.User;
import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.http.client.HttpRedirects;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DashboardControllerTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired UserRepository userRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired PasswordEncoder passwordEncoder;

    @BeforeEach
    void setUp() {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));
        userRepository.save(new User(organization.getId(), "alice", passwordEncoder.encode("secret123")));
    }

    @AfterEach
    void tearDown() {
        userRepository.deleteAll();
        organizationRepository.deleteAll();
    }

    @Test
    void 로그인_페이지는_200을_반환한다() {
        ResponseEntity<String> response = restTemplate.getForEntity(url("/login"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void 세션_없이_대시보드에_접근하면_로그인으로_리다이렉트된다() {
        ResponseEntity<String> response = restTemplate.withRedirects(HttpRedirects.DONT_FOLLOW)
                .getForEntity(url("/dashboard"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FOUND);
        assertThat(response.getHeaders().getLocation().getPath()).isEqualTo("/login");
    }

    @Test
    void 로그인_후_대시보드에_접근하면_사용자명이_노출된다() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, loginAndGetSessionCookie());

        ResponseEntity<String> response = restTemplate.exchange(
                url("/dashboard"), HttpMethod.GET, new HttpEntity<>(headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("alice");
    }

    private String loginAndGetSessionCookie() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", "alice");
        form.add("password", "secret123");

        ResponseEntity<Void> response = restTemplate.withRedirects(HttpRedirects.DONT_FOLLOW).exchange(
                url("/login"), HttpMethod.POST, new HttpEntity<>(form, headers), Void.class);

        return response.getHeaders().get(HttpHeaders.SET_COOKIE).get(0).split(";")[0];
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
```

- [ ] **Step 2: 컴파일/실행 실패 확인**

```bash
./gradlew test --tests "com.example.credit_system.dashboard.DashboardControllerTest"
```

Expected: FAIL — `DashboardController`, 템플릿 없음 (컴파일은 되지만 `/login`, `/dashboard` 요청 시 템플릿을 찾지 못해 500)

- [ ] **Step 3: DashboardController 구현**

```java
package com.example.credit_system.dashboard;

import com.example.credit_system.global.auth.SessionConst;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;

@Controller
public class DashboardController {

    @GetMapping("/dashboard")
    public String dashboard(@RequestAttribute(SessionConst.USERNAME) String username, Model model) {
        model.addAttribute("username", username);
        return "dashboard";
    }
}
```

- [ ] **Step 4: login.html 작성**

`src/main/resources/templates/login.html`:

```html
<!DOCTYPE html>
<html lang="ko" xmlns:th="http://www.thymeleaf.org">
<head>
    <meta charset="UTF-8">
    <title>로그인 - Credit System</title>
</head>
<body>
<h1>Credit System 로그인</h1>
<form method="post" action="/login">
    <label>아이디 <input type="text" name="username" required></label><br>
    <label>비밀번호 <input type="password" name="password" required></label><br>
    <button type="submit">로그인</button>
</form>
<p th:if="${param.error}" style="color:red">아이디 또는 비밀번호가 올바르지 않습니다.</p>
</body>
</html>
```

- [ ] **Step 5: dashboard.html 작성**

`src/main/resources/templates/dashboard.html`:

```html
<!DOCTYPE html>
<html lang="ko" xmlns:th="http://www.thymeleaf.org">
<head>
    <meta charset="UTF-8">
    <title>Credit System 대시보드</title>
    <style>
        body { font-family: sans-serif; max-width: 800px; margin: 2rem auto; }
        table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
        th, td { border: 1px solid #ccc; padding: 4px 8px; text-align: left; }
        .hidden { display: none; }
    </style>
</head>
<body>
<h1>Credit System 대시보드</h1>
<p>로그인: <strong th:text="${username}">username</strong></p>

<section>
    <h2>잔액: <span id="balance">-</span></h2>
    <form id="charge-form">
        <input type="number" id="charge-amount" min="1" value="1000" required>
        <button type="submit">충전</button>
    </form>
</section>

<section>
    <h2>새 생성 요청</h2>
    <form id="job-form">
        <input type="text" id="prompt" placeholder="예: a cat wearing sunglasses" required style="width:60%">
        <button type="submit" id="submit-btn">요청</button>
    </form>
</section>

<div>
    <button onclick="showTab('jobs')">Job 목록</button>
    <button onclick="showTab('ledger')">Ledger 내역</button>
</div>

<section id="jobs-tab">
    <table>
        <thead><tr><th>ID</th><th>상태</th><th>시도</th><th>차감액</th><th>프롬프트</th><th>결과</th></tr></thead>
        <tbody id="jobs-body"></tbody>
    </table>
</section>

<section id="ledger-tab" class="hidden">
    <table>
        <thead><tr><th>ID</th><th>타입</th><th>금액</th><th>Job ID</th><th>시각</th></tr></thead>
        <tbody id="ledger-body"></tbody>
    </table>
</section>

<script>
async function loadBalance() {
    const res = await fetch('/api/organizations/me/balance');
    const data = await res.json();
    document.getElementById('balance').textContent = data.balance;
}

async function loadJobs() {
    const res = await fetch('/api/jobs');
    const jobs = await res.json();
    document.getElementById('jobs-body').innerHTML = jobs.map(job => `
        <tr>
            <td>${job.id}</td>
            <td>${job.status}</td>
            <td>${job.attemptNo}</td>
            <td>${job.holdAmount}</td>
            <td>${job.prompt}</td>
            <td>${job.resultUrl ?? ''}</td>
        </tr>`).join('');
}

async function loadLedger() {
    const res = await fetch('/api/ledger');
    const entries = await res.json();
    document.getElementById('ledger-body').innerHTML = entries.map(entry => `
        <tr>
            <td>${entry.id}</td>
            <td>${entry.type}</td>
            <td>${entry.amount}</td>
            <td>${entry.jobId ?? ''}</td>
            <td>${entry.createdAt}</td>
        </tr>`).join('');
}

function showTab(name) {
    document.getElementById('jobs-tab').classList.toggle('hidden', name !== 'jobs');
    document.getElementById('ledger-tab').classList.toggle('hidden', name !== 'ledger');
    if (name === 'ledger') {
        loadLedger();
    }
}

document.getElementById('charge-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const amount = Number(document.getElementById('charge-amount').value);
    await fetch('/api/organizations/me/charge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ amount })
    });
    await loadBalance();
});

document.getElementById('job-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const submitBtn = document.getElementById('submit-btn');
    submitBtn.disabled = true;
    try {
        const prompt = document.getElementById('prompt').value;
        const idemKey = crypto.randomUUID();
        await fetch('/api/jobs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ idemKey, prompt })
        });
        document.getElementById('prompt').value = '';
        await loadJobs();
        await loadBalance();
    } finally {
        submitBtn.disabled = false;
    }
});

loadBalance();
loadJobs();
setInterval(() => { loadJobs(); loadBalance(); }, 3000);
</script>
</body>
</html>
```

멱등키(`idemKey`)는 `crypto.randomUUID()`로 매 제출 시 새로 만들고, 제출 버튼은 응답이 올 때까지 비활성화해 같은 클릭으로 인한 중복 제출을 막는다. 네트워크 재전송에 대한 진짜 멱등성 검증은 Task 17의 `DuplicateIdemKeyTest`와 Task 19 README의 curl 예시로 확인한다.

- [ ] **Step 6: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.dashboard.DashboardControllerTest"
```

Expected: 3 tests PASS

- [ ] **Step 7: 커밋**

```bash
git add src/main/java/com/example/credit_system/dashboard src/main/resources/templates \
        src/test/java/com/example/credit_system/dashboard
git commit -m "feat: add dashboard UI with polling job/ledger views"
```

---

### Task 17: 동시성 통합 테스트 (Testcontainers MySQL)

이 태스크가 이 프로젝트의 핵심 주장 — "크레딧은 항상 정확하게 차감·환불된다" — 을 실제 MySQL 위에서 증명한다. 지금까지의 모든 테스트는 H2였다; 실제 InnoDB 행 잠금/유니크 제약 하에서도 동작을 재검증한다.

**Files:**
- Create: `src/test/java/com/example/credit_system/job/concurrency/ConcurrentHoldTest.java`
- Create: `src/test/java/com/example/credit_system/job/concurrency/DuplicateIdemKeyTest.java`
- Create: `src/test/java/com/example/credit_system/job/concurrency/RetryRefundTest.java`

**Interfaces:**
- Consumes: `HoldService` (Task 9), `JobRepository`/`OrganizationRepository`/`LedgerRepository` (Task 3, 4), `DeadJobSchedulerTask`/`OutboxRelay`/`GenerationWorker` (Task 10~12, 실제로 켠 채로 실행)
- 이 태스크는 새 production 코드를 만들지 않는다 — 지금까지 만든 모든 조각이 실제 MySQL(+Kafka+Redis)에서도 맞물려 동작하는지 검증하는 테스트만 추가한다

- [ ] **Step 1: ConcurrentHoldTest 작성 — 동시 hold 요청이 잔액을 음수로 만들지 않는다**

`src/test/java/com/example/credit_system/job/concurrency/ConcurrentHoldTest.java`:

```java
package com.example.credit_system.job.concurrency;

import com.example.credit_system.global.exception.BalanceConflictException;
import com.example.credit_system.global.exception.InsufficientBalanceException;
import com.example.credit_system.job.service.HoldService;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class ConcurrentHoldTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit");

    @DynamicPropertySource
    static void datasourceProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
    }

    @Autowired HoldService holdService;
    @Autowired OrganizationRepository organizationRepository;

    @Test
    void 동시에_여러_요청이_들어와도_잔액이_음수가_되지_않는다() throws InterruptedException {
        // app.generation.cost=100(테스트 프로파일 기본값), 잔액 500 → 최대 5건만 성공 가능
        Organization organization = organizationRepository.save(new Organization("acme", 500L));

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger rejectedCount = new AtomicInteger();

        for (int i = 0; i < threadCount; i++) {
            int idx = i;
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    holdService.requestGeneration(organization.getId(), "concurrent-key-" + idx, "cat");
                    successCount.incrementAndGet();
                } catch (InsufficientBalanceException | BalanceConflictException e) {
                    rejectedCount.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        start.countDown();
        done.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(successCount.get() + rejectedCount.get()).isEqualTo(threadCount);
        assertThat(successCount.get()).isLessThanOrEqualTo(5);

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isGreaterThanOrEqualTo(0);
        assertThat(found.getBalance()).isEqualTo(500L - successCount.get() * 100L);
    }
}
```

`successCount`의 정확한 값(이상적으로는 5)을 단언하지 않고 "잔액과 성공 건수가 항상 정합한다"만 단언하는 이유: 10개 스레드가 동시에 같은 row를 두고 경쟁하면 낙관적 락 재시도(최대 3회)를 모두 소진하는 극단적 스케줄링도 이론상 가능하다. 이 프로젝트가 실제로 보장하는 것은 "정확히 5건 성공"이 아니라 "잔액은 절대 음수가 되지 않고, 성공 건수와 차감액이 항상 일치한다"이므로 그 불변식만 검증한다.

- [ ] **Step 2: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.concurrency.ConcurrentHoldTest"
```

Expected: 1 test PASS (MySQL 컨테이너 기동으로 10~30초 정도 걸릴 수 있음)

- [ ] **Step 3: DuplicateIdemKeyTest 작성 — 동일 idemKey 동시 요청은 job을 1개만 만든다**

`src/test/java/com/example/credit_system/job/concurrency/DuplicateIdemKeyTest.java`:

```java
package com.example.credit_system.job.concurrency;

import com.example.credit_system.global.exception.DuplicateRequestInProgressException;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.HoldService;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class DuplicateIdemKeyTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit");

    @DynamicPropertySource
    static void datasourceProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
    }

    @Autowired HoldService holdService;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired OrganizationRepository organizationRepository;

    @Test
    void 동일_idemKey로_동시_요청해도_job과_차감은_한_번만_일어난다() throws InterruptedException {
        Organization organization = organizationRepository.save(new Organization("acme", 10_000L));
        String idemKey = "shared-key";

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    holdService.requestGeneration(organization.getId(), idemKey, "cat");
                } catch (DuplicateRequestInProgressException e) {
                    // 다른 스레드가 idempotency key를 선점한 직후, job 연결 전의 극히 짧은 race window에 걸린 경우.
                    // 클라이언트가 같은 idemKey로 재시도하면 결국 성공 응답을 받게 되므로 이 테스트에선 무시한다.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        start.countDown();
        done.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(jobRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).hasSize(1);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId())).hasSize(1);

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(10_000L - 100L);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.concurrency.DuplicateIdemKeyTest"
```

Expected: 1 test PASS

- [ ] **Step 5: RetryRefundTest 작성 — 실제 파이프라인 전체를 통한 3회 실패 → 최종 환불**

이 테스트는 `app.stub.failure-rate=1.0`로 워커가 항상 실패하도록 강제해, Hold→Outbox→Kafka→Worker(항상 실패)→DeadJobSchedulerTask(재시도)→...→최종 REFUNDED까지 **실제 배선 그대로** 관통시킨다. Task 18의 E2E 테스트가 "항상 성공" 경로를 검증한다면, 이 테스트는 그 반대 경로(실패→재시도→환불)를 같은 방식으로 검증하는 짝이다.

`src/test/java/com/example/credit_system/job/concurrency/RetryRefundTest.java`:

```java
package com.example.credit_system.job.concurrency;

import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.HoldResult;
import com.example.credit_system.job.service.HoldService;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.annotation.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = "generation-jobs")
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.scheduling.enabled=true",
        "app.worker.enabled=true",
        "app.stub.failure-rate=1.0",
        "app.scheduling.outbox-relay-interval-millis=200",
        "app.scheduling.dead-job-scan-interval-millis=500"
})
class RetryRefundTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired HoldService holdService;
    @Autowired JobRepository jobRepository;
    @Autowired OrganizationRepository organizationRepository;
    @Autowired LedgerRepository ledgerRepository;

    @Test
    void 매번_실패하면_재시도를_모두_소진하고_최종적으로_환불된다() {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));

        HoldResult result = holdService.requestGeneration(organization.getId(), "retry-key", "cat");

        // app.generation.max-attempts=3, 조건이 "attemptNo < 3"이므로 attemptNo 0,1,2인 시점에 각각 재시도가 한 번씩 더 일어나
        // 실제로는 초기 시도 포함 총 4번(attemptNo 0→1→2→3) 시도된 뒤 attemptNo=3에서 최종 환불된다.
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            var job = jobRepository.findById(result.jobId()).orElseThrow();
            assertThat(job.getStatus()).isEqualTo(JobStatus.REFUNDED);
            assertThat(job.getAttemptNo()).isEqualTo(3);
        });

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(1000L);
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .extracting(entry -> entry.getType().name())
                .contains("HOLD", "REFUND");
    }
}
```

- [ ] **Step 6: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.concurrency.RetryRefundTest"
```

Expected: 1 test PASS (MySQL+Redis+EmbeddedKafka 기동과 4회 왕복으로 다른 테스트보다 오래 걸릴 수 있음, 그래도 30초 이내)

- [ ] **Step 7: 전체 테스트 스위트 한 번에 실행**

```bash
./gradlew test
```

Expected: 지금까지 만든 모든 테스트 PASS

- [ ] **Step 8: 커밋**

```bash
git add src/test/java/com/example/credit_system/job/concurrency
git commit -m "test: add concurrency integration tests against real MySQL"
```

---

### Task 18: E2E 파이프라인 테스트 (성공 경로)

Task 17의 `RetryRefundTest`가 "항상 실패" 경로를 실제 배선으로 검증했다면, 이 테스트는 그 반대인 "항상 성공" 경로를 `HoldService` 호출부터 시작해 outbox→Kafka→worker→confirm까지 실제로 관통시켜 검증한다 — 지금까지 만든 모든 조각이 하나의 요청 안에서 실제로 맞물려 동작하는지 확인하는 캡스톤 테스트다.

**Files:**
- Create: `src/test/java/com/example/credit_system/job/concurrency/GenerationPipelineEndToEndTest.java`

**Interfaces:**
- Consumes: `HoldService` (Task 9), `OutboxRelay` (Task 10), `GenerationWorker`/`ConfirmService` (Task 11), 전부 실제로 켠 채로 실행
- 새 production 코드 없음 — 테스트만 추가

- [ ] **Step 1: 테스트 작성**

`src/test/java/com/example/credit_system/job/concurrency/GenerationPipelineEndToEndTest.java`:

```java
package com.example.credit_system.job.concurrency;

import com.example.credit_system.job.domain.JobStatus;
import com.example.credit_system.job.repository.JobRepository;
import com.example.credit_system.job.service.HoldResult;
import com.example.credit_system.job.service.HoldService;
import com.example.credit_system.ledger.repository.LedgerRepository;
import com.example.credit_system.organization.domain.Organization;
import com.example.credit_system.organization.repository.OrganizationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.annotation.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = "generation-jobs")
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.scheduling.enabled=true",
        "app.worker.enabled=true",
        "app.stub.failure-rate=0.0",
        "app.scheduling.outbox-relay-interval-millis=200"
})
class GenerationPipelineEndToEndTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("credit_system")
            .withUsername("credit")
            .withPassword("credit");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired HoldService holdService;
    @Autowired JobRepository jobRepository;
    @Autowired LedgerRepository ledgerRepository;
    @Autowired OrganizationRepository organizationRepository;

    @Test
    void hold_요청부터_컨펌까지_전체_파이프라인이_실제로_동작한다() {
        Organization organization = organizationRepository.save(new Organization("acme", 1000L));

        HoldResult result = holdService.requestGeneration(
                organization.getId(), "e2e-key", "a cat wearing sunglasses");

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            var job = jobRepository.findById(result.jobId()).orElseThrow();
            assertThat(job.getStatus()).isEqualTo(JobStatus.COMPLETED);
            assertThat(job.getResultUrl()).isNotNull();
        });

        Organization found = organizationRepository.findById(organization.getId()).orElseThrow();
        assertThat(found.getBalance()).isEqualTo(900L); // confirm은 잔액을 바꾸지 않음 — hold 시점 차감이 그대로 유지
        assertThat(ledgerRepository.findByOrganizationIdOrderByIdDesc(organization.getId()))
                .extracting(entry -> entry.getType().name())
                .contains("HOLD", "CONFIRM");
    }
}
```

- [ ] **Step 2: 테스트 통과 확인**

```bash
./gradlew test --tests "com.example.credit_system.job.concurrency.GenerationPipelineEndToEndTest"
```

Expected: 1 test PASS

- [ ] **Step 3: 커밋**

```bash
git add src/test/java/com/example/credit_system/job/concurrency/GenerationPipelineEndToEndTest.java
git commit -m "test: add full pipeline end-to-end success test"
```

---

### Task 19: README, 로컬 실행 확인, 전체 검증

**Files:**
- Create: `README.md`

**Interfaces:**
- 새 production 코드 없음 — 문서화와 최종 검증만

- [ ] **Step 1: README.md 작성**

`README.md` (프로젝트 루트):

```markdown
# Credit System

Organization이 공유하는 크레딧을 선결제/차감하고, 비동기 이미지 생성(stub) 실패 시 정확히 환불하는 것을 목표로 한 포트폴리오 프로젝트입니다.
핵심 설계 원칙과 흐름은 [`docs/superpowers/specs/2026-07-04-credit-system-design.md`](docs/superpowers/specs/2026-07-04-credit-system-design.md)에 정리돼 있습니다.

이미지 생성 자체는 이 프로젝트의 관심사가 아니라서 지연+확률적 실패를 가진 stub(`GenerationStubClient`)으로 대체돼 있습니다.
핵심은 "크레딧이 항상 정확하게 차감·환불되는가"이며, 이는 조건부 UPDATE(낙관적 락 + fencing token)와
`docs/superpowers/plans/2026-07-04-credit-system.md`의 Task 17/18에 있는 Testcontainers 기반 동시성 테스트로 증명합니다.

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
```

- [ ] **Step 2: 커밋**

```bash
git add README.md
git commit -m "docs: add README with run instructions and idempotency demo"
```

- [ ] **Step 3: 전체 검증 (docker-compose 실제 기동)**

```bash
docker compose up -d
./gradlew bootRun
```

다른 터미널에서:

```bash
curl -c /tmp/cookies.txt -X POST http://localhost:8080/login -d "username=alice&password=password123" -i
```

응답이 `302`이고 `Location: /dashboard`인지 확인한다. 브라우저로 `http://localhost:8080/login` 접속 → `alice`/`password123` 로그인 → 대시보드에서:

1. 잔액이 10,000으로 보이는지
2. 프롬프트를 입력해 생성 요청을 제출하면 job 목록에 `HOLDING`→`PROCESSING`→(`COMPLETED` 또는 `FAILED`→재시도) 순으로 상태가 바뀌는지 (3초 폴링)
3. Ledger 탭에서 HOLD/CONFIRM 또는 HOLD/REFUND 기록이 남는지
4. 충전 버튼으로 잔액이 올라가는지

를 직접 눈으로 확인한다. 확인이 끝나면:

```bash
docker compose down
```

- [ ] **Step 4: 전체 자동 테스트 마지막 실행**

```bash
./gradlew clean test
```

Expected: 모든 태스크(1~19)에서 작성한 테스트가 PASS. 실패하는 테스트가 있다면 해당 태스크로 돌아가 원인을 고친다.
```
