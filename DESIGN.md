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

## 10. SLO Definitions

Three SLOs are defined for the production PayBridge service. Each includes the signal being measured, the target, and the alerting strategy.

---

### SLO 1 — Payment Success Rate ≥ 99.5% (30-day rolling window)

**What we measure**

The fraction of payment creation requests that result in a terminal `Completed` or `Submitted` (pending async confirmation) status, excluding requests rejected by fraud screening (fraud rejection is expected behaviour, not a system error).

```
success_rate = 1 - (
  rate(paybridge_payments_created_total{status="Failed"}[30d])
  /
  rate(paybridge_payments_created_total[30d])
)
```

**Target:** 99.5% — no more than 0.5% of payments fail due to system errors (provider unavailability, DB errors, infrastructure faults) over any 30-day window.

**Error budget:** 0.5% of requests over 30 days ≈ 216 minutes of total failure time at 1 req/s.

**Alerting**

| Severity | Condition | Action |
|---|---|---|
| Page (P1) | Error rate > 2% for 5 minutes | Immediate on-call response — circuit breaker likely open or provider down |
| Ticket (P2) | Error rate > 0.5% for 30 minutes | Error budget burn rate > 1× — investigate before budget is exhausted |
| Burn-rate alert | 5% of 30-day budget consumed in 1 hour | Fast burn — escalate if trend continues |

---

### SLO 2 — Payment API p99 Latency ≤ 2 seconds (synchronous path)

**What we measure**

End-to-end response time for `POST /api/payments` — from request receipt to `201 Created` response. This covers the synchronous path: idempotency check → fraud gRPC → provider HTTP → Kafka publish. The async webhook/settlement leg is excluded.

```
histogram_quantile(0.99,
  sum(rate(paybridge_payment_processing_duration_seconds_bucket[5m])) by (le)
)
```

**Target:** p99 ≤ 2 seconds. The 2-second budget is allocated as: fraud gRPC ≤ 300ms, provider HTTP ≤ 1200ms (including one retry), Postgres writes ≤ 100ms, Redis ≤ 50ms, overhead ≤ 350ms.

**Error budget:** 1% of requests per month may exceed 2s ≈ ~14,400 slow requests at 1 req/s.

**Alerting**

| Severity | Condition | Action |
|---|---|---|
| Page (P1) | p99 > 5s for 3 minutes | Provider circuit breaker likely open or DB overloaded |
| Ticket (P2) | p99 > 2s for 10 minutes | Latency SLO at risk — check fraud service and provider tail latency |
| Warning | p95 > 1.5s for 15 minutes | Early signal before p99 breaches — review provider response times |

---

### SLO 3 — Settlement Lag ≤ 60 seconds (p95, end-to-end)

**What we measure**

Time from when a `PaymentCompleted` or `PaymentFailed` webhook is received by `WebhookReceiver` to when the corresponding `SettlementRecord` is persisted. This covers: webhook dedup check (Redis) → Kafka publish → Kafka consumer poll → DB upsert.

Approximated as:
```
histogram_quantile(0.95,
  sum(rate(paybridge_settlement_processing_duration_seconds_bucket[5m])) by (le)
)
```

For deeper end-to-end measurement, compare `SettlementRecord.PersistedAt - PaymentEvent.Timestamp` in Postgres.

**Target:** p95 settlement lag ≤ 60 seconds. The dominant driver is Kafka consumer poll interval (default 1s) plus DB write time. Breaching this SLO indicates consumer lag is building or the settlement consumer is stopped.

**Error budget:** 5% of settlements per month may exceed 60s.

**Alerting**

| Severity | Condition | Action |
|---|---|---|
| Page (P1) | No settlements persisted in 5 minutes despite incoming webhooks | Consumer is stopped or Kafka consumer group is stuck |
| Ticket (P2) | p95 settlement lag > 60s for 10 minutes | Consumer throughput degraded — check Kafka consumer lag metric |
| Warning | `paybridge_settlement_records_persisted_total` rate = 0 for 2 minutes | Possible consumer crash — check pod restarts |

---

## 11. Incident Runbook — Payment Success Rate Dropped

**Trigger:** P1 alert fires — `paybridge_payments_created_total{status="Failed"}` error rate exceeds 2% for 5 consecutive minutes.

---

### Step 1 — Confirm scope (2 minutes)

```bash
# Is the drop recent and sharp, or a slow trend?
# Check Grafana → PayBridge Overview → "Payments by Status" panel

# Is it affecting all merchants or one?
# Query Prometheus:
sum by (status) (rate(paybridge_payments_created_total[5m]))
```

If error rate is 100%: likely a full infrastructure failure (Postgres down, Redis unavailable).
If error rate is 5–20%: likely provider-side failures or fraud service degradation.

---

### Step 2 — Check the provider circuit breaker (3 minutes)

The most common cause of elevated failure rates is the provider circuit breaker opening.

```bash
# Look for circuit breaker log events:
docker logs deploy-payment-api-1 2>&1 | grep "Circuit breaker"

# Check provider stub health:
curl -s http://localhost:8082/health

# Check payment-api logs for provider errors:
docker logs deploy-payment-api-1 2>&1 | grep -E "Provider|provider" | tail -20
```

**If the circuit breaker is open:**
- Wait for the 60-second break duration to elapse (automatic recovery)
- If provider remains unhealthy after recovery: activate the kill switch to stop new payments from accumulating failures while the provider incident is resolved
  ```bash
  redis-cli SET flags:payment_processing false
  ```
- Notify merchants via status page

---

### Step 3 — Check Postgres and Redis (3 minutes)

```bash
# Health endpoints:
curl -s http://localhost:8080/health/ready   # checks both Postgres + Redis

# Postgres connectivity:
docker logs deploy-postgres-1 2>&1 | tail -20

# Redis connectivity:
docker exec deploy-redis-1 redis-cli ping
```

**If Postgres is down:** payments will fail at the initial `INSERT`. Activate kill switch to prevent error storm, restore Postgres, then re-enable.

**If Redis is down:** idempotency and feature flag checks will fail. The `RedisIdempotencyService` does not have a fallback — payments will return 500. Restore Redis or temporarily swap `IIdempotencyService` to a no-op implementation.

---

### Step 4 — Check the fraud service (2 minutes)

```bash
docker logs deploy-fraud-stub-1 2>&1 | tail -20
curl -s http://localhost:8081/health
```

The fraud service is **fail-open** — a timeout or error causes payments to proceed with `RiskScore=0.5`. Fraud service failure alone should not cause payment failures. If fraud is returning errors AND payments are failing, the failure is happening downstream of fraud.

---

### Step 5 — Check for Outbox backlog (2 minutes)

If payments are succeeding but events are not being delivered, the Kafka producer may be failing silently and the outbox is filling up.

```bash
# Check outbox backlog size:
docker exec deploy-postgres-1 psql -U paybridge -c \
  'SELECT COUNT(*) FROM "OutboxEvents" WHERE "ProcessedAt" IS NULL;'

# Check outbox worker errors:
docker logs deploy-payment-api-1 2>&1 | grep "Outbox" | tail -10
```

If the backlog exceeds 1,000 rows, Kafka is likely down or unreachable. Restore Kafka — the OutboxWorker will drain automatically.

---

### Mitigation Summary

| Root Cause | Mitigation |
|---|---|
| Provider circuit breaker open | Wait for auto-recovery; kill switch if provider is down for > 5 min |
| Postgres unavailable | Restore Postgres; kill switch to halt new requests during recovery |
| Redis unavailable | Restore Redis; consider no-op idempotency fallback |
| Kafka unavailable | Outbox guarantees no event loss; restore Kafka to drain backlog |
| Fraud service crashing | No action needed (fail-open); alerts for fraud are tagged `degraded` |

---

## 12. PII and Data Governance

PayBridge processes three categories of sensitive data: customer PII (email address), financial data (payment amount, currency, method), and merchant data (merchant ID, tenant ID).

### 12.1 Customer Email

**Storage:** Customer email is **never persisted** to the database. The `CreatePaymentRequest` record carries the email only in memory during the synchronous request path.

**Transmission:** Before the email is sent to the fraud service, `FraudGrpcClient` hashes it using SHA-256 and truncates to 16 hex characters (`customer_email_hash`). Only this one-way hash crosses the wire. The original email is not included in any Kafka event, outbox record, or structured log field.

**Logs and traces:** No log statement in any service references `CustomerEmail` directly. OTel span attributes carry `payment.merchant_id`, `payment.currency`, and `payment.method` — never cardholder data. The OTel Collector pipeline includes an `attributes/drop_pii` processor that explicitly deletes `db.statement` and `http.request.header.authorization` from all metric streams.

### 12.2 Payment Amounts

**Storage:** `Amount` is stored as `DECIMAL(18,4)` in both `Payments` and `SettlementRecords`. This is financial data, not PII, but it is sensitive.

**Metrics:** Amount values are never used as metric label dimensions. Metrics use bucketed enumerations (`risk_bucket: low/medium/high/critical`, `method: CreditCard/...`, `status: Completed/Failed`). This prevents high-cardinality label explosion and avoids leaking individual transaction amounts into the metrics pipeline.

**Traces:** OTel span attributes include `payment.amount` for the payment creation span. In a production deployment, this attribute should be removed or replaced with an amount range (e.g., `payment.amount_bucket: 0-100/100-1000/1000+`) to prevent financial data appearing in trace storage systems that may have different retention and access controls than the primary database.

### 12.3 Merchant Data

Merchant IDs and tenant IDs are considered non-sensitive operational identifiers and are safe to include in logs, traces, and metrics labels. They carry no cardholder information.

### 12.4 Retention

| Data | Storage | Retention |
|---|---|---|
| Payment records | Postgres | Indefinite (configurable per-tenant via partition or archival job) |
| Settlement records | Postgres | Indefinite (financial audit trail) |
| Idempotency cache | Redis | 24 hours (auto-expiry) |
| Trace context (Redis) | Redis | 48 hours (auto-expiry) |
| Webhook dedup keys | Redis | 7 days (auto-expiry) |
| Distributed traces | Jaeger | Default: in-memory, no persistence. Production: configure OTEL backend with 30-day retention |
| Metrics | Prometheus | Default: local TSDB. Production: configure remote_write to long-term store (e.g., Thanos, Cortex) with 13-month retention |

### 12.5 Access Controls (Production Guidance)

- Postgres: per-service credentials with least-privilege (PaymentApi has INSERT/UPDATE on Payments and OutboxEvents; SettlementConsumer has INSERT on SettlementRecords, UPDATE on Payments; no service has DELETE)
- Redis: ACL rules restricting each service to its own key prefix
- Kafka: mTLS between brokers and clients; per-service ACLs on the `payment-events` topic
- OTel Collector: runs inside the private network; OTLP ports (4317/4318) are not exposed externally

---

## 13. Cost Awareness at 1,000 Payments/Minute

At 1,000 payments/minute (≈ 16.7 req/s), the primary observability cost drivers are trace volume, metric cardinality, and log throughput.

### 13.1 Trace Volume

Each payment creates approximately 8–10 spans:
- PaymentApi: 1 root span + 1 fraud span + 1 provider span + 1 Kafka publish
- WebhookReceiver: 1 span
- SettlementConsumer: 1 span
- Postgres/Redis/gRPC auto-instrumentation: 3–4 spans

At 1,000 payments/minute → ~10,000 spans/minute → ~600,000 spans/hour.

**Cost mitigations:**
- **Head-based sampling:** Apply a 10–20% sample rate for non-error traces using the OTel Collector's `probabilistic_sampler` processor. Error traces and fraud-rejected traces remain at 100% to preserve debuggability. This reduces span volume by 80–90% while keeping full fidelity on failures.
- **Tail-based sampling (preferred):** Use the OTel Collector's `tail_sampling` processor to keep 100% of traces that contain an error span, and sample down to 5% of all-success traces. This requires buffering spans in the collector (memory cost) but eliminates the "sampled away an interesting trace" problem.
- **Span attribute pruning:** The `attributes/drop_pii` processor already removes `db.statement`. In production, also drop `http.url` (high cardinality from UUIDs in payment ID paths) and replace with a templated route attribute (`/api/payments/{id}`).

### 13.2 Metric Cardinality

The six custom metrics use low-cardinality label sets (`status`, `method`, `result`, `risk_bucket`). At 1,000 payments/minute, these generate approximately:
- `payments_created_total`: 4 label combinations (status) × 4 (method) = 16 time series
- `fraud_checks_total`: 2 (result) × 4 (risk_bucket) = 8 time series
- All others: O(10) time series each

Total custom metric series: < 100. This is negligible for any Prometheus-compatible backend.

**Risk to watch:** ASP.NET Core's default HTTP metrics include `http.route` as a label. A route like `/api/payments/{paymentId}` without template matching would create one time series per unique payment ID — at 1,000 payments/minute this would generate millions of series within hours. The current setup uses route-template instrumentation (ASP.NET Core OTel integration normalises the route), so this is safe. Verify `http_server_request_duration_seconds` labels in Prometheus before deploying.

### 13.3 Log Volume

At 1,000 payments/minute, structured logging produces approximately:
- PaymentApi: ~5 log lines per payment = 5,000 lines/minute
- WebhookReceiver: ~3 lines per webhook = 3,000 lines/minute
- SettlementConsumer: ~4 lines per event = 4,000 lines/minute
- Total: ~12,000 lines/minute ≈ 720,000 lines/hour

At an average of 200 bytes per line, this is ~144 MB/hour or ~3.4 GB/day. In a managed log aggregation service (e.g., Datadog, Loki), this is the largest variable cost.

**Cost mitigations:**
- Suppress EF Core query logs below `Warning` — already implemented.
- Set health-check endpoint logs to `Debug` or exclude them from the OTel pipeline — already filtered from traces (`opt.Filter = ctx => !ctx.Request.Path.StartsWithSegments("/health")`). Apply the same filter to logs.
- In production, route `Information`-level logs to a cheap cold store (S3 + Athena) and send only `Warning`+  to the hot log aggregation system.
- Use log sampling for high-frequency `PaymentInitiated` events: log 1-in-10 at `Information`, always log at `Warning`+.

### 13.4 Kafka Retention

At 1,000 payments/minute, the `payment-events` topic produces:
- ~3 events per payment (Initiated + Completed/Failed) = 3,000 events/minute
- Average event size: ~300 bytes → ~900 KB/minute → ~1.3 GB/day

Set topic retention to 7 days (≈ 9 GB) with log compaction disabled (events are append-only). In a managed Kafka service, this is the secondary cost driver after traces.

### 13.5 Summary Table

| Component | Volume at 1k pay/min | Primary mitigation |
|---|---|---|
| Traces | ~600k spans/hr | Tail-based sampling: 100% errors, 5% successes |
| Metrics | < 100 time series | Already low-cardinality; monitor `http.route` label |
| Logs | ~720k lines/hr (144 MB) | Cold-tier for Info, hot-tier for Warning+ |
| Kafka | ~1.3 GB/day | 7-day retention; no compaction needed |
| Postgres | ~1,440 payment rows/hr | Partition by month; archive after 90 days |

---

## 14. Known Limitations and Future Work

- **No authentication/authorization** on the PaymentApi. Production deployments would add JWT/mTLS validation and per-merchant API keys.
- **Single Kafka partition** per payment key. Adding partitioned consumers with consumer group rebalancing requires careful offset management.
- **OutboxWorker does not dead-letter** after 5 retries. A production system should move exhausted events to a dead-letter queue and alert.
- **ProviderStub is stateless** — it never persists the submitted payment, so querying by `providerTransactionId` is unsupported. A real provider integration would expose a status query endpoint.
- **Settlement consumer enrichment** falls back to `"unknown"` for `MerchantId`/`TenantId` if the webhook fires before the PaymentApi writes the row. A retry or event-sourcing approach would handle this race condition.
- **No schema registry** for Kafka events. Adding Avro or Protobuf schemas with a schema registry would enforce backward-compatible evolution.
- **Redis is a single point of failure** for idempotency, feature flags, and webhook dedup. Production deployments should use Redis Sentinel or Redis Cluster.
- **Feature flags are coarse-grained** — only a global kill switch exists. A per-merchant or per-method flag system would allow safer gradual rollouts.
