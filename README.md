# 🔄 Retail Renewal Service

> Automated end-to-end policy renewal platform powering **10,000+ renewals/month** across a 6-stage lifecycle — built with Java, Spring Boot, Kafka, PostgreSQL, and Redis.

![Java](https://img.shields.io/badge/Java-17-ED8B00?style=flat-square&logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.x-6DB33F?style=flat-square&logo=springboot&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white)
![Swagger](https://img.shields.io/badge/Swagger-OpenAPI_3.0-85EA2D?style=flat-square&logo=swagger&logoColor=black)

---

## 📊 Impact at a Glance

| Metric | Result |
|---|---|
| Policies processed | **10,000+/month** |
| Manual effort reduced | **~60%** |
| Kafka renewal events/day | **5,000+** |
| API integration uptime | **99.5%** |
| SLA breach rate reduced | **45%** |
| Renewal stages automated | **6** |

---

## 🏗️ Architecture Overview

```
                        ┌─────────────────────────────────────────┐
                        │         Retail Renewal Service           │
                        │                                          │
  Inbound Request ──►   │  ┌──────────┐     ┌──────────────────┐  │
                        │  │   REST   │────►│  Renewal Stage   │  │
  External APIs  ──►    │  │   APIs   │     │    Orchestrator  │  │
                        │  └──────────┘     └────────┬─────────┘  │
                        │                            │            │
                        │                   ┌────────▼─────────┐  │
                        │                   │   Stage Engine   │  │
                        │                   │  1. Initiation   │  │
                        │                   │  2. Eligibility  │  │
                        │                   │  3. Premium Calc │  │
                        │                   │  4. Endorsement  │  │
                        │                   │  5. Payment      │  │
                        │                   │  6. Completion   │  │
                        │                   └────────┬─────────┘  │
                        │                            │            │
                        │         ┌──────────────────▼──────────┐ │
                        │         │        Kafka Producer        │ │
                        │         │   (5,000+ events/day)        │ │
                        │         └──────────────────┬──────────┘ │
                        └────────────────────────────┼────────────┘
                                                      │
               ┌──────────────────────────────────────▼────────────┐
               │                  Kafka Topics                      │
               │  renewal.initiated  |  renewal.completed  | ...   │
               └────────────┬─────────────────┬──────────────┬─────┘
                            │                 │              │
                     ┌──────▼──────┐  ┌───────▼──────┐  ┌───▼──────────┐
                     │  Payment    │  │ Notification  │  │  Audit Log   │
                     │  Service    │  │   Service     │  │   Service    │
                     └─────────────┘  └──────────────┘  └──────────────┘

Data Layer:
  PostgreSQL  ──  Policy state, audit trail, renewal history
  Redis Cache ──  Active renewal sessions, premium lookups (~35% DB read reduction)
```

---

## ✨ Key Features

- **Step-based orchestration** — 6-stage renewal lifecycle with state tracking and rollback support
- **Event-driven with Kafka** — fault-tolerant downstream sync with zero data loss across all consumer services
- **10+ REST API integrations** — real-time premium recalculation, endorsements, and payment initiation (Swagger/OpenAPI documented)
- **Redis caching layer** — reduces database read load and speeds up frequently accessed renewal data
- **99.5% integration uptime** — resilient API client with retry, circuit-breaker, and timeout handling
- **Observability-ready** — structured logging, exception handling, and CloudWatch-compatible metrics

---

## 🗂️ Project Structure

```
retail-renewal-service/
├── src/
│   └── main/
│       └── java/com/insurance/renewal/
│           ├── controller/          # REST API controllers
│           │   └── RenewalController.java
│           ├── service/             # Business logic & orchestration
│           │   ├── RenewalOrchestrationService.java
│           │   └── StageProcessorService.java
│           ├── kafka/               # Kafka producers & consumers
│           │   ├── RenewalEventProducer.java
│           │   └── RenewalEventConsumer.java
│           ├── repository/          # JPA repositories
│           │   └── RenewalRepository.java
│           ├── model/               # Entities & DTOs
│           │   ├── Policy.java
│           │   ├── RenewalRecord.java
│           │   └── RenewalStage.java
│           └── config/              # Kafka, Redis, datasource config
│               ├── KafkaConfig.java
│               └── RedisConfig.java
├── docs/
│   └── renewal-flow.puml            # PlantUML sequence diagram
├── application.properties.example
└── README.md
```

---

## 🔑 Core Components

### Renewal Stage Orchestrator (stub)

```java
@Service
@Slf4j
public class RenewalOrchestrationService {

    private final Map<RenewalStage, StageProcessor> stageProcessors;
    private final RenewalEventProducer eventProducer;

    public RenewalResult processRenewal(String policyId, RenewalStage currentStage) {
        log.info("Processing policy {} at stage {}", policyId, currentStage);
        StageProcessor processor = stageProcessors.get(currentStage);
        RenewalResult result = processor.process(policyId);
        if (result.isSuccess()) {
            eventProducer.publishRenewalEvent(policyId, currentStage, result);
        }
        return result;
    }
}
```

### Kafka Event Producer (stub)

```java
@Component
public class RenewalEventProducer {

    private final KafkaTemplate<String, RenewalEvent> kafkaTemplate;

    public void publishRenewalEvent(String policyId, RenewalStage stage, RenewalResult result) {
        RenewalEvent event = RenewalEvent.builder()
                .policyId(policyId)
                .stage(stage)
                .status(result.getStatus())
                .timestamp(Instant.now())
                .build();
        kafkaTemplate.send("renewal.events", policyId, event);
    }
}
```

### REST Controller (stub)

```java
@RestController
@RequestMapping("/api/v1/renewals")
@Tag(name = "Renewal API", description = "Policy renewal lifecycle management")
public class RenewalController {

    @PostMapping("/{policyId}/initiate")
    @Operation(summary = "Initiate renewal for a policy")
    public ResponseEntity<RenewalResponse> initiateRenewal(@PathVariable String policyId) {
        // ...
    }

    @GetMapping("/{policyId}/status")
    @Operation(summary = "Get current renewal stage and status")
    public ResponseEntity<RenewalStatusResponse> getRenewalStatus(@PathVariable String policyId) {
        // ...
    }
}
```

---

## 🔄 Renewal Lifecycle

```
  [1] Initiation ──► [2] Eligibility Check ──► [3] Premium Recalculation
                                                          │
  [6] Completion ◄── [5] Payment Processing ◄── [4] Endorsement
```

Each stage is independently processable — if a stage fails, the renewal record retains its last successful state and can be retried without reprocessing completed stages.

---

## ⚙️ Configuration

Copy `application.properties.example` to `application.properties` and fill in your values:

```properties
# Server
server.port=8080

# PostgreSQL
spring.datasource.url=jdbc:postgresql://localhost:5432/renewal_db
spring.datasource.username=YOUR_DB_USER
spring.datasource.password=YOUR_DB_PASSWORD

# Redis
spring.redis.host=localhost
spring.redis.port=6379

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=renewal-service-group
kafka.topic.renewal-events=renewal.events

# Cache TTL (seconds)
cache.renewal.ttl=300
```

---

## 🚀 Running Locally

**Prerequisites:** Java 17+, Docker (for Kafka, PostgreSQL, Redis)

```bash
# 1. Clone the repo
git clone https://github.com/deepti-pujari/retail-renewal-service.git
cd retail-renewal-service

# 2. Start dependencies
docker-compose up -d

# 3. Configure
cp application.properties.example src/main/resources/application.properties

# 4. Build & run
./mvnw spring-boot:run
```

API docs available at: `http://localhost:8080/swagger-ui.html`

---

## 🧪 Testing

```bash
# Run unit + integration tests
./mvnw test

# Run with coverage report
./mvnw verify
```

Test coverage includes JUnit 5 unit tests for stage processors and Mockito-based mocks for Kafka and repository layers.

---

## 📄 API Reference

Full API spec available via Swagger UI. Core endpoints:

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/renewals/{policyId}/initiate` | Start the renewal lifecycle |
| `GET` | `/api/v1/renewals/{policyId}/status` | Get current stage & status |
| `POST` | `/api/v1/renewals/{policyId}/advance` | Advance to the next stage |
| `POST` | `/api/v1/renewals/{policyId}/retry` | Retry a failed stage |
| `GET` | `/api/v1/renewals/{policyId}/history` | Full audit trail |

---

## 👩‍💻 Author

**Deepti Pujari** — Java Software Development Engineer  
[LinkedIn](https://linkedin.com/in/deepti-pujari) · [GitHub](https://github.com/deepti-pujari) · [deeptipujari02@gmail.com](mailto:deeptipujari02@gmail.com)

---

> *This repository contains sanitized code stubs and documentation. Production source code is proprietary to Star Health & Allied Insurance Co. Ltd.*
