# PayBridge — System Design Document

## 1. Problem Statement

PayBridge is a payment processing gateway that must:

- Accept payment requests from merchants and route them to a downstream payment provider
- Screen every payment for fraud before submission
- Guarantee that each payment is processed exactly once even under retries, network failures, or client crashes
- Reconcile async provider outcomes (delivered via webhook) against the original payment record
- Produce an auditable settlement record for every terminal event
- Expose structured logs, distributed traces, and business metrics from all services

## 2. High-Level Architecture

```
                        ┌─────────────────────────────────────────────────┐
                        │                 PayBridge.PaymentApi              │
Client ──POST /payments─▶   FeatureFlags  Idempotency  PaymentService       │
                        │        │             │              │              │
                        │        ▼             ▼              ▼              │
                        │     Redis          Redis     ┌──FraudGrpcClient─▶ FraudStub (gRPC)
                        │                             │  ProviderHttpClient▶ ProviderStub (HTTP)
                        │                             │  KafkaProducer ─────▶ Kafka
                        │                             │  OutboxWorker ◀────── OutboxEvents (Postgres)
                        └─────────────────────────────┘
                                                                 │ (async webhook)
                                                                 ▼
                                                      WebhookReceiver (HTTP)
                                                           Redis (dedup)
                                                           Kafka ─────────▶ SettlementConsumer
                                                                                    │
                                                                                    ▼
                                                                               PostgreSQL
                                                                          (Payments + SettlementRecords)
```

## 3. Service Responsibilities

### 3.1 PayBridge.PaymentApi

The single entry point for all payment creation and status queries.

**Request path:**

1. Check the kill switch (Redis key `flags:payment_processing`).
2. Check the idempotency store (Redis) — return cached result immediately on hit.
3. Persist a `Payment` row at status `FraudChecking`.
4. Store the current OTel trace ID in Redis for later webhook linking.
5. Call the fraud service via gRPC.
6. On fraud approval, call the payment provider via HTTP.
7. On provider acceptance, publish a `PaymentInitiated` event to Kafka (with transactional outbox fallback).
8. Cache the response in Redis under the idempotency key.
9. Return `201 Created`.

**Notable design choices:**

- The fraud check is **fail-open**: a timeout or RPC error returns `approved=true` with a 0.5 risk score. This avoids blocking payments when the fraud service is degraded.
- The provider HTTP client is wrapped in a **Polly resilience pipeline** (retry → circuit breaker → timeout). The pipeline prevents cascading failures from a slow provider.
- Kafka publish failures are handled by writing to an **outbox table** in the same database transaction. A background `OutboxWorker` retries delivery with a max of 5 attempts.

### 3.2 PayBridge.FraudStub

A gRPC service implementing the `FraudDetection` protocol (defined in `fraud.proto`). In production this would be replaced by a real ML-based scorer. The stub randomizes a `riskScore` and rejects the top 20% of scores.

### 3.3 PayBridge.ProviderStub

A minimal HTTP service simulating a payment network. After accepting a `/payments/submit` request it fires an async webhook to `WebhookReceiver` after a random 500–2000ms delay, mimicking real provider settlement latency.

### 3.4 PayBridge.WebhookReceiver

Receives `POST /webhooks/provider` callbacks from the provider. Responsibilities:

1. **Deduplication** — Redis `SET NX` on `webhook:processed:{providerTxId}` (7-day TTL). Duplicate callbacks return `200 already_processed` without re-publishing.
2. **Trace linking** — The webhook arrives as a new root HTTP request with no trace parent. The receiver reads the stored trace ID from Redis and attaches it as an `ActivityLink`, allowing Jaeger to display the two traces as related.
3. **Event fan-out** — Publishes a `PaymentCompleted` or `PaymentFailed` `PaymentEvent` to the `payment-events` Kafka topic.

### 3.5 PayBridge.SettlementConsumer

A Kafka consumer (`GroupId: settlement-consumer`, `EnableAutoCommit: false`) that reads `payment-events` and persists only terminal events (`PaymentCompleted`, `PaymentFailed`).

Key behaviors:

- **Idempotent upsert**: Uses `INSERT … ON CONFLICT ("PaymentId") DO NOTHING` so that re-delivered Kafka messages never create duplicate settlement records.
- **Enrichment**: When the event originates from the webhook path, `MerchantId`/`TenantId`/`Amount` may be `"unknown"`. The consumer looks up the full `Payment` record from Postgres and uses those values instead.
- **Manual offset commit**: The Kafka offset is only committed after the database write succeeds, providing at-least-once delivery semantics with idempotent processing.
- **W3C context extraction**: Trace context is extracted from Kafka message headers so the consumer span is a child of the publisher's span.

## 4. Data Model

### 4.1 Payments Table

```
Payment {
    Id              GUID        PK
    MerchantId      TEXT        NOT NULL
    TenantId        TEXT        NOT NULL
    IdempotencyKey  TEXT        NOT NULL
    Amount          DECIMAL(18,4)
    Currency        TEXT
    Method          TEXT        (enum stored as string)
    Status          TEXT        (enum stored as string)
    ProviderTransactionId TEXT  NULLABLE
    FailureReason   TEXT        NULLABLE
    CreatedAt       TIMESTAMPTZ
    CompletedAt     TIMESTAMPTZ NULLABLE
    OriginalTraceId TEXT        NULLABLE

    UNIQUE (MerchantId, IdempotencyKey)
}
```

### 4.2 SettlementRecords Table

```
SettlementRecord {
    Id                    BIGINT      PK AUTOINCREMENT
    PaymentId             GUID        UNIQUE (prevents double settlement)
    MerchantId            TEXT
    TenantId              TEXT
    Amount                DECIMAL(18,4)
    Currency              TEXT
    FinalStatus           TEXT
    ProviderTransactionId TEXT        NULLABLE
    EventTimestamp        TIMESTAMPTZ
    PersistedAt           TIMESTAMPTZ

    UNIQUE (PaymentId)
}
```

### 4.3 OutboxEvents Table

```
OutboxEvent {
    Id          BIGINT PK AUTOINCREMENT
    Topic       TEXT
    Payload     TEXT    (JSON-serialized PaymentEvent)
    TraceContext TEXT   NULLABLE ("traceId:spanId")
    CreatedAt   TIMESTAMPTZ
    RetryCount  INT
    ProcessedAt TIMESTAMPTZ NULLABLE
    INDEX (ProcessedAt)
}
```

### 4.4 Redis Key Space

| Key pattern | TTL | Purpose |
|---|---|---|
| `idem:{merchantId}:{key}` | 24h | Idempotency response cache |
| `flags:payment_processing` | — (manual) | Kill switch — value `"false"` disables processing |
| `trace:{paymentId}` | 48h | `traceId:spanId` for webhook trace linking |
| `webhook:processed:{providerTxId}` | 7 days | Webhook deduplication NX key |

## 5. Event Schema

### PaymentEvent (Kafka: `payment-events`)

```json
{
  "PaymentId": "uuid",
  "MerchantId": "string",
  "TenantId": "string",
  "EventType": "PaymentInitiated | PaymentCompleted | PaymentFailed",
  "Amount": 99.99,
  "Currency": "USD",
  "ProviderTransactionId": "string | null",
  "FailureReason": "string | null",
  "Timestamp": "ISO-8601"
}
```

W3C trace context (`traceparent`, `tracestate`) is injected as Kafka message headers by the publisher and extracted by the consumer.

## 6. Resilience Patterns

### 6.1 Transactional Outbox

The `KafkaProducer` writes events to Kafka in the happy path. If Kafka is unavailable it falls back to writing an `OutboxEvent` row to Postgres inside the same transaction that saved the `Payment` record, ensuring the event is never lost.

The `OutboxWorker` (a hosted background service) polls `OutboxEvents` every 5 seconds for unprocessed rows (up to 50 at a time) and re-delivers them. After 5 failed retries a row is left unprocessed for manual investigation.

### 6.2 Provider Resilience Pipeline (Polly)

```
Request ──▶ Retry (3 attempts, exponential jitter, 200ms base)
        ──▶ Circuit Breaker (opens at 50% failure / 30s window / min 5 req, breaks 60s)
        ──▶ Timeout (8s)
        ──▶ HttpClient (10s global timeout)
```

Circuit breaker events are logged at `Warning` level and surfaced as structured log fields.

### 6.3 Fraud Service Fail-Open

The gRPC client sets a 3-second deadline. On `DeadlineExceeded` or any `RpcException`, it returns a synthetic `Approved=true, RiskScore=0.5` response. This prevents the fraud service from becoming a hard dependency for payment throughput.

### 6.4 Idempotency

The idempotency key is scoped per merchant: `idem:{merchantId}:{clientKey}`. This prevents key collisions between merchants submitting similar keys (e.g., `"order-1"`). The cache TTL is 24 hours to cover client retries within a business day.

### 6.5 Webhook Deduplication

The payment provider may fire the same webhook multiple times (network retry, provider bug). The receiver uses `SETNX` in Redis with a 7-day TTL. Duplicates are ack'd with `200 already_processed` without touching Kafka or Postgres.

## 7. Observability

### 7.1 Distributed Tracing

Each service registers its own `ActivitySource`. Spans are exported via OTLP gRPC to the OpenTelemetry Collector, which forwards them to Jaeger.

**Trace propagation paths:**

- HTTP (REST + webhook): standard W3C `traceparent` header via ASP.NET Core instrumentation
- gRPC: OpenTelemetry gRPC client instrumentation
- Kafka: manual `Propagators.DefaultTextMapPropagator` inject/extract on message headers

**Async span linking:**

The `WebhookReceiver` creates a new root trace for each inbound webhook (the provider has no context to propagate). It reads the stored `trace:{paymentId}` from Redis, parses the `traceId:spanId`, and attaches it as an `ActivityLink`. Jaeger renders these as linked traces, enabling end-to-end visibility across the synchronous and asynchronous payment legs.

### 7.2 Metrics

| Metric | Type | Description |
|---|---|---|
| `payments_created_total` | Counter | Total payments, labelled `status` + `method` |
| `fraud_checks_total` | Counter | Fraud outcomes, labelled `result` + `risk_bucket` |
| `payment_processing_duration_seconds` | Histogram | Full create path latency |
| `settlement_records_persisted_total` | Counter | Settlements written, labelled `status` |
| `settlement_processing_duration_seconds` | Histogram | Kafka-to-DB latency |
| `webhooks_received_total` | Counter | Webhooks received, labelled `status` |

All services also expose default ASP.NET Core and HTTP client metrics via OpenTelemetry SDK instrumentation.

### 7.3 Structured Logging

All services use Serilog with `Enrich.FromLogContext()` and `Enrich.WithMachineName()`. All log statements that reference a payment include `PaymentId` as a structured property, enabling correlation in any log aggregation system.

EF Core query logs are suppressed below `Warning` to avoid noise.

### 7.4 Health Checks

| Endpoint | Purpose | Checks |
|---|---|---|
| `/health/live` | Liveness | Process is running (no dependency checks) |
| `/health/ready` | Readiness | PostgreSQL + Redis healthy |
| `/health` | Full | All checks including fraud service (degraded tag) |

Fraud service is tagged `degraded` (not `critical`) — the system can still route payments without it.

## 8. Security Considerations

### 8.1 PII Handling

Customer email is never stored in the database or sent downstream. The `FraudGrpcClient` hashes the email with SHA-256 and truncates to 16 hex characters before including it in the gRPC request. Only the hash is transmitted.

### 8.2 Multi-Tenancy

Each payment carries a `TenantId` derived from the `MerchantId` prefix. In the current implementation this is a simple split — in production it would be a lookup against a merchant configuration service. Settlement records inherit the tenant ID for future per-tenant reporting and data isolation.

## 9. Payment Status State Machine

```
                   ┌──────────────────────────────┐
                   │                              │
Created ──▶ FraudChecking ──fraud fail──▶ Failed  │
                   │                              │
              fraud pass                          │
                   ▼                              │
             Submitted ──provider fail──▶ Failed  │
                   │                              │
              webhook ──▶ Completed               │
                   │                              │
              webhook ──▶ Failed ─────────────────┘
```

`Refunded` is defined in the enum but not yet wired up to a flow.

## 10. Known Limitations and Future Work

- **No authentication/authorization** on the PaymentApi. Production deployments would add JWT/mTLS validation and per-merchant API keys.
- **Single Kafka partition** per payment key. Adding partitioned consumers with consumer group rebalancing requires careful offset management.
- **OutboxWorker does not dead-letter** after 5 retries. A production system should move exhausted events to a dead-letter queue and alert.
- **ProviderStub is stateless** — it never persists the submitted payment, so querying by `providerTransactionId` is unsupported. A real provider integration would expose a status query endpoint.
- **Settlement consumer enrichment** falls back to `"unknown"` for `MerchantId`/`TenantId` if the webhook fires before the PaymentApi writes the row. A retry or event-sourcing approach would handle this race condition.
- **No schema registry** for Kafka events. Adding Avro or Protobuf schemas with a schema registry would enforce backward-compatible evolution.
- **Redis is a single point of failure** for idempotency, feature flags, and webhook dedup. Production deployments should use Redis Sentinel or Redis Cluster.
- **Feature flags are coarse-grained** — only a global kill switch exists. A per-merchant or per-method flag system would allow safer gradual rollouts.
