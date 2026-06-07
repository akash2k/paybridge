# PayBridge

A production-grade distributed payment processing system built with .NET microservices. PayBridge demonstrates an end-to-end payment lifecycle — from REST submission through fraud screening, provider routing, async webhook reconciliation, and settlement — with full observability via OpenTelemetry, Jaeger, Prometheus, and Grafana.

## Architecture

```
Client
  │
  ▼
PaymentApi (8080)           ──gRPC──▶  FraudStub (8081)
  │                         ──HTTP──▶  ProviderStub (8082)
  │  Kafka: payment-events             │
  ├──────────────────────────────────────▶ (async webhook)
  │                                    ▼
  │                         WebhookReceiver (8083)
  │                                    │ Kafka: payment-events
  ▼                                    ▼
PostgreSQL ◀── SettlementConsumer ◀───────────────────
Redis (idempotency, feature flags, webhook dedup)
```

### Services

| Service | Port | Role |
|---|---|---|
| `PayBridge.PaymentApi` | 8080 | Core REST API — orchestrates the full payment flow |
| `PayBridge.FraudStub` | 8081 | gRPC fraud detection stub (fail-open on timeout/error) |
| `PayBridge.ProviderStub` | 8082 | HTTP payment provider stub with async webhook callbacks |
| `PayBridge.WebhookReceiver` | 8083 | Receives provider callbacks, deduplicates, re-publishes to Kafka |
| `PayBridge.SettlementConsumer` | 8084 | Kafka consumer — persists terminal events as settlement records |

### Infrastructure

| Component | Port | Purpose |
|---|---|---|
| PostgreSQL 16 | 5432 | Payments table, SettlementRecords table, Outbox table |
| Redis 7 | 6379 | Idempotency cache, feature flags, webhook dedup |
| Kafka (Confluent 7.6) | 9092 | `payment-events` topic |
| OpenTelemetry Collector | 4317/4318 | OTLP aggregation → Jaeger + Prometheus |
| Jaeger | 16686 | Distributed trace UI |
| Prometheus | 9090 | Metrics scraping |
| Grafana | 3000 | Metrics dashboards (admin/admin) |

## Quick Start

```bash
cd deploy
docker compose up --build
```

All services start with health-check-gated `depends_on`, so they come up in the correct order. The PaymentApi auto-migrates the database on startup.

Once running:

- **Swagger UI**: http://localhost:8080/swagger
- **Jaeger UI**: http://localhost:16686
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000

### Create a Payment

```bash
curl -X POST http://localhost:8080/api/payments \
  -H "Content-Type: application/json" \
  -d '{
    "merchantId": "merchant_acme",
    "idempotencyKey": "order-42",
    "amount": 99.99,
    "currency": "USD",
    "customerEmail": "user@example.com",
    "method": "CreditCard"
  }'
```

Returns `201 Created` with a `PaymentResponse`, or `200 OK` if the idempotency key was already seen.

### Get Payment Status

```bash
curl http://localhost:8080/api/payments/{paymentId}
```

### Kill Switch (disable all payment processing)

```bash
# Disable
docker exec -it <redis-container> redis-cli SET flags:payment_processing false

# Re-enable
docker exec -it <redis-container> redis-cli DEL flags:payment_processing
```

## Payment Lifecycle

1. **Kill switch check** — Redis key `flags:payment_processing` (30s local cache)
2. **Idempotency check** — Redis key `idem:{merchantId}:{idempotencyKey}` (24h TTL)
3. **Persist payment** — Postgres, status = `FraudChecking`
4. **Fraud check** — gRPC to FraudStub, 3s deadline; timeout/error = fail-open
5. **Provider submit** — HTTP to ProviderStub with Polly retry + circuit breaker
6. **Publish `PaymentInitiated`** — Kafka with outbox fallback if Kafka is down
7. **Async webhook** — ProviderStub fires callback 500–2000ms later
8. **Webhook dedup** — Redis NX key `webhook:processed:{providerTxId}` (7-day TTL)
9. **Publish `PaymentCompleted` / `PaymentFailed`** — Kafka
10. **Settlement** — Kafka consumer upserts to `SettlementRecords`, updates payment status

## Resilience

| Mechanism | Where | Detail |
|---|---|---|
| Retry with jitter | PaymentApi → ProviderStub | 3 attempts, exponential backoff, 200ms base delay |
| Circuit breaker | PaymentApi → ProviderStub | Opens at 50% failure over 30s (min 5 req), breaks for 60s |
| Timeout | PaymentApi → ProviderStub | 8s Polly timeout + 10s HttpClient timeout |
| Fail-open fraud | FraudGrpcClient | gRPC deadline exceeded → approved=true, risk=0.5 |
| Transactional outbox | KafkaProducer | Kafka failure → write to `OutboxEvents` table; OutboxWorker retries (max 5) every 5s |
| Manual offset commit | SettlementConsumer | Commit only after successful DB write; at-least-once delivery |
| Webhook deduplication | WebhookReceiver | Redis NX prevents duplicate settlement events |
| Idempotency | PaymentApi | Re-submitting same idempotency key returns cached response |

## Observability

### Metrics

All services export OTLP metrics to Prometheus:

| Metric | Labels |
|---|---|
| `payments_created_total` | `status`, `method` |
| `fraud_checks_total` | `result`, `risk_bucket` |
| `payment_processing_duration_seconds` | `stage` |
| `settlement_records_persisted_total` | `status` |
| `settlement_processing_duration_seconds` | — |
| `webhooks_received_total` | `status` |

### Tracing

Distributed traces span all five services. The WebhookReceiver creates a new root trace for each inbound webhook but uses `ActivityLink` to link back to the original payment trace (trace ID is stored in Redis for 48h). This allows a single Jaeger search to connect the synchronous and asynchronous halves of a payment.

W3C trace context is propagated through Kafka message headers.

### Structured Logs

All services log with Serilog to stdout in structured JSON format. Key log events carry `PaymentId`, `MerchantId`, and status fields for easy filtering.

### Health Checks

```
GET /health/live   — liveness (process alive)
GET /health/ready  — readiness (Postgres + Redis healthy)
GET /health        — full (all checks)
```

## Project Structure

```
paybridge/
├── deploy/
│   ├── docker-compose.yml
│   ├── otel-collector-config.yml
│   └── prometheus.yml
├── src/
│   ├── PayBridge.Shared/           # Shared models, events, proto definitions
│   │   ├── Models/                 # Payment, SettlementRecord, OutboxEvent
│   │   ├── Events/                 # PaymentEvent, ProviderWebhookCallback
│   │   └── Protos/                 # fraud.proto (gRPC contract)
│   ├── PayBridge.PaymentApi/       # Core REST API
│   │   ├── Controllers/            # PaymentsController
│   │   ├── Services/               # PaymentService, FraudGrpcClient, etc.
│   │   └── Infrastructure/         # AppDbContext, KafkaProducer
│   ├── PayBridge.FraudStub/        # gRPC fraud detection stub
│   ├── PayBridge.ProviderStub/     # HTTP provider stub
│   ├── PayBridge.WebhookReceiver/  # Provider webhook ingestion
│   └── PayBridge.SettlementConsumer/ # Kafka-to-Postgres settlement writer
└── tests/
    └── PayBridge.UnitTests/        # PaymentService unit tests (xUnit + Moq)
```

## Configuration

All services are configured via environment variables (or `appsettings.json` locally). The docker-compose file provides all values for containerized runs.

| Variable | Default | Service |
|---|---|---|
| `ConnectionStrings__Postgres` | `Host=postgres;...` | PaymentApi, SettlementConsumer |
| `Redis__ConnectionString` | `redis:6379` | PaymentApi, WebhookReceiver |
| `Kafka__BootstrapServers` | `kafka:9092` | PaymentApi, WebhookReceiver, SettlementConsumer |
| `FraudService__Address` | `http://fraud-stub:8081` | PaymentApi |
| `ProviderStub__BaseUrl` | `http://provider-stub:8082` | PaymentApi |
| `WebhookReceiver__Url` | `http://webhook-receiver:8083` | ProviderStub |
| `Otel__Endpoint` | `http://otel-collector:4317` | All services |

## Running Tests

```bash
dotnet test tests/PayBridge.UnitTests
```

Tests use an in-memory EF Core database and Moq mocks for all external dependencies.
