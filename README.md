# PayBridge

A production-grade distributed payment processing system built with .NET 8 microservices. Demonstrates an end-to-end payment lifecycle — REST submission → fraud screening (gRPC) → provider routing (HTTP) → async webhook reconciliation → Kafka-driven settlement — with full OpenTelemetry observability.

> **Design document:** [`DESIGN.md`](DESIGN.md) covers architecture rationale, SLO definitions, incident runbook, PII governance, and cost analysis.

---

## Quick Start

```bash
git clone https://github.com/akash2k/paybridge.git
cd paybridge
docker compose up --build
```

Services start in dependency order via health-check-gated `depends_on`. The first run pulls images and builds all five .NET services — allow 3–5 minutes. On subsequent runs with cached layers it takes under 60 seconds.

**Once running:**

| UI | URL | Credentials |
|---|---|---|
| Swagger (payment API) | http://localhost:8080/swagger | — |
| Jaeger (distributed traces) | http://localhost:16686 | — |
| Grafana (metrics dashboards) | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

---

## Architecture

```
                                   ┌──────────────────────────────────────────────┐
                                   │           PayBridge.PaymentApi :8080          │
                                   │                                               │
 Client ──POST /api/payments──────▶│  1. Kill switch  (Redis)                     │
 Client ──GET  /api/payments/{id}──│  2. Idempotency  (Redis)                     │
                                   │  3. Persist      (Postgres: Payments)         │──gRPC──▶ FraudStub :8081
                                   │  4. Fraud check  (gRPC, fail-open)           │
                                   │  5. Submit       (HTTP + Polly resilience)    │──HTTP──▶ ProviderStub :8082
                                   │  6. Publish      (Kafka / Outbox fallback)   │               │
                                   └────────────────────────────────┬─────────────┘               │
                                                                     │                    async webhook (500-2000ms)
                                              Kafka: payment-events  │                             │
                                                                     │                             ▼
                                                                     │              WebhookReceiver :8083
                                                                     │               • Redis dedup (NX, 7d TTL)
                                                                     │               • Trace link (ActivityLink)
                                                                     │               • Re-publish to Kafka
                                                                     │                             │
                                                                     │         Kafka: payment-events│
                                                                     ▼                             ▼
                                                            SettlementConsumer :8084  ◀────────────┘
                                                             • Manual offset commit
                                                             • Idempotent upsert (ON CONFLICT DO NOTHING)
                                                             • Enrich from Payments table
                                                                     │
                                                                     ▼
                                                              Postgres: SettlementRecords


Infrastructure
──────────────
PostgreSQL 16 :5432    Redis 7 :6379    Kafka (Confluent 7.6) :9092

Observability stack
───────────────────
All services ──OTLP gRPC──▶ OTel Collector :4317
                                  │
                    ┌─────────────┼──────────────┐
                    ▼             ▼               ▼
               Jaeger :16686  Prometheus :9090  (debug logs)
                                  │
                             Grafana :3000
```

### Services

| Service | Port | Technology | Role |
|---|---|---|---|
| `PayBridge.PaymentApi` | 8080 | ASP.NET Core 8, EF Core, Polly | Core REST API — orchestrates the full synchronous payment flow |
| `PayBridge.FraudStub` | 8081 | ASP.NET Core 8, gRPC | Fraud detection stub — randomised risk score, 20% rejection rate |
| `PayBridge.ProviderStub` | 8082 | ASP.NET Core 8 | Payment network stub — 92% success, fires async webhook |
| `PayBridge.WebhookReceiver` | 8083 | ASP.NET Core 8, Kafka | Receives provider callbacks, deduplicates, re-publishes events |
| `PayBridge.SettlementConsumer` | 8084 | .NET Worker, Kafka, EF Core | Consumes terminal events, writes settlement records |

---

## Sending a Payment and Viewing the Trace

### Step 1 — Create a payment

```bash
curl -X POST http://localhost:8080/api/payments \
  -H "Content-Type: application/json" \
  -d '{
    "merchantId": "merchant_acme",
    "idempotencyKey": "order-001",
    "amount": 99.99,
    "currency": "USD",
    "customerEmail": "user@example.com",
    "method": "CreditCard"
  }'
```

Response (201 Created):
```json
{
  "paymentId": "7db33d9e-b282-4868-9649-a3e0c2b6b299",
  "status": "Submitted",
  "providerTransactionId": "prov_8da7743f625c",
  "failureReason": null,
  "createdAt": "2026-06-07T13:28:47Z"
}
```

Wait 2–3 seconds, then check the final status (webhook + settlement completes async):

```bash
curl http://localhost:8080/api/payments/7db33d9e-b282-4868-9649-a3e0c2b6b299
# status will be "Completed" or "Failed"
```

### Step 2 — View the end-to-end trace in Jaeger

1. Open **http://localhost:16686**
2. In the **Service** dropdown select `PayBridge.PaymentApi`
3. Click **Find Traces** — the most recent trace appears at the top
4. Click the trace to expand it

You will see spans across all services:
- `PayBridge.PaymentApi` — root span covering the full synchronous path
  - `payment.fraud_check` — gRPC call to FraudStub
  - `payment.provider_submit` — HTTP call to ProviderStub
- `PayBridge.WebhookReceiver` — a **linked** trace (new root, linked via `ActivityLink`)
- `PayBridge.SettlementConsumer` — child of the WebhookReceiver Kafka publish span

The synchronous and async legs are separate traces linked by `ActivityLink` — click **"1 linked trace"** in Jaeger to jump between them.

### Step 3 — View metrics in Grafana

1. Open **http://localhost:3000** (admin / admin)
2. Navigate to **Dashboards → PayBridge Overview**
3. Panels show: payments by status, fraud check outcomes, processing latency (p50/p99), webhooks received, and settlement throughput

---

## Payment Lifecycle

```
POST /api/payments
  │
  ├─ 1. Kill switch check       Redis key flags:payment_processing  (30s cached)
  ├─ 2. Idempotency check       Redis key idem:{merchantId}:{key}   (24h TTL)
  ├─ 3. Persist Payment row     Postgres  status=FraudChecking
  ├─ 4. Fraud check             gRPC → FraudStub  3s deadline, fail-open on error
  ├─ 5. Provider submit         HTTP → ProviderStub  retry+CB+timeout (Polly)
  ├─ 6. Publish PaymentInitiated  Kafka (outbox fallback if Kafka unavailable)
  └─ Return 201 Created

  ... async (500–2000ms later) ...

  ProviderStub fires webhook → WebhookReceiver
    │
    ├─ Dedup check   Redis NX  webhook:processed:{providerTxId}  (7d TTL)
    ├─ Trace link    reads trace:{paymentId} from Redis → ActivityLink
    ├─ Publish PaymentCompleted / PaymentFailed  → Kafka
    └─ Return 200

  SettlementConsumer reads Kafka
    │
    ├─ Idempotent upsert  INSERT ... ON CONFLICT (PaymentId) DO NOTHING
    ├─ Enrich from Payments table  (MerchantId, Amount, Currency)
    ├─ Update Payments.Status → Completed / Failed
    └─ Commit Kafka offset
```

---

## Key Design Decisions and Trade-offs

### Fail-open fraud service
The fraud gRPC client has a 3-second deadline. On timeout or error it returns `approved=true, risk=0.5` and allows the payment to proceed. **Trade-off:** A prolonged fraud service outage means elevated risk exposure. The alternative — fail-closed — would block all payments when fraud is degraded, which is a worse business outcome for most scenarios. The 0.5 risk score is logged and tracked in metrics so anomalies are visible.

### Transactional outbox for Kafka
Rather than writing to Postgres and Kafka in a two-phase pattern, the `KafkaProducer` falls back to writing an `OutboxEvent` row to Postgres in the same DB transaction when Kafka is unavailable. A background `OutboxWorker` retries delivery. **Trade-off:** Introduces eventual consistency (events may be delayed by up to 5s) and adds a polling worker. The benefit is that no payment is ever lost because Kafka was momentarily down at the time of creation.

### Async webhook with trace linking, not distributed context propagation
The payment provider fires webhooks as new HTTP requests with no trace context. Rather than forcing an artificial parent-child relationship, `WebhookReceiver` starts a fresh root trace and uses `ActivityLink` to reference the original payment trace. **Trade-off:** Jaeger shows two separate traces rather than one continuous waterfall — but this accurately represents the async boundary. The linked trace navigation in Jaeger provides full end-to-end visibility.

### Idempotency scoped per merchant
The Redis idempotency key is `idem:{merchantId}:{clientKey}`, not just `idem:{clientKey}`. This prevents two merchants submitting `idempotencyKey: "order-1"` from colliding. **Trade-off:** Requires the client to include their merchant ID consistently on every retry (which they must anyway for routing).

### Manual Kafka offset commit in SettlementConsumer
The consumer commits the Kafka offset only after the database write succeeds. This gives at-least-once delivery semantics. Duplicate delivery is handled by the `ON CONFLICT DO NOTHING` upsert, making the consumer idempotent. **Trade-off:** If the consumer crashes between the DB write and the commit, the message is re-processed — but the upsert makes this safe.

### EnsureCreatedAsync instead of migrations
The current deployment uses `EnsureCreatedAsync()` to create the schema on first boot. **Trade-off:** Schema changes require dropping and recreating the database. In production, EF Core migrations with a migration runner would be used instead. This is a deliberate simplification for the demo environment.

---

## Resilience Summary

| Mechanism | Component | Detail |
|---|---|---|
| Retry with jitter | PaymentApi → ProviderStub | 3 attempts, exponential backoff, 200ms base |
| Circuit breaker | PaymentApi → ProviderStub | Opens at 50% failure / 30s window, breaks for 60s |
| Timeout | PaymentApi → ProviderStub | 8s Polly + 10s HttpClient |
| Fail-open | FraudGrpcClient | Timeout/error → approved=true, risk=0.5 |
| Transactional outbox | KafkaProducer + OutboxWorker | Kafka down → DB fallback, retry every 5s (max 5 attempts) |
| At-least-once + idempotent upsert | SettlementConsumer | Manual offset commit + `ON CONFLICT DO NOTHING` |
| Webhook deduplication | WebhookReceiver | Redis `SET NX` with 7-day TTL |
| Idempotency | PaymentApi | `idem:{merchantId}:{key}` cached 24h in Redis |
| Kill switch | PaymentApi | Redis key `flags:payment_processing = false` halts all processing |

---

## Running Tests

```bash
dotnet test tests/PayBridge.UnitTests
```

Five unit tests covering `PaymentService`:
- Idempotency cache hit — skips fraud and provider calls entirely
- Fraud rejection — payment fails without calling provider
- Provider acceptance — `PaymentInitiated` event published to Kafka
- Kill switch active — throws `InvalidOperationException`
- Kafka failure propagation behaviour

Tests use an in-memory EF Core database and Moq mocks for all external dependencies.

---

## Project Structure

```
paybridge/
├── docker-compose.yml              ← single-command startup (run from here)
├── deploy/
│   ├── docker-compose.yml          ← deploy-directory variant (same stack)
│   ├── otel-collector-config.yml   ← OTLP → Jaeger + Prometheus pipelines
│   ├── prometheus.yml              ← scrape config
│   └── grafana/provisioning/       ← auto-wired datasource + PayBridge dashboard
├── src/
│   ├── PayBridge.Shared/           ← Models, Events, fraud.proto
│   ├── PayBridge.PaymentApi/       ← REST API (Controllers, Services, Infrastructure)
│   ├── PayBridge.FraudStub/        ← gRPC fraud service
│   ├── PayBridge.ProviderStub/     ← HTTP provider + webhook fire
│   ├── PayBridge.WebhookReceiver/  ← Webhook ingestion + Kafka publish
│   └── PayBridge.SettlementConsumer/ ← Kafka consumer + settlement persistence
└── tests/
    └── PayBridge.UnitTests/        ← xUnit + Moq unit tests
```

---

## Configuration

All values are wired in `docker-compose.yml`. For local development without Docker, set these environment variables or update `appsettings.json`:

| Variable | Default | Used by |
|---|---|---|
| `ConnectionStrings__Postgres` | `Host=postgres;Port=5432;Database=paybridge;Username=paybridge;Password=secret` | PaymentApi, SettlementConsumer |
| `Redis__ConnectionString` | `redis:6379` | PaymentApi, WebhookReceiver |
| `Kafka__BootstrapServers` | `kafka:9092` | PaymentApi, WebhookReceiver, SettlementConsumer |
| `FraudService__Address` | `http://fraud-stub:8081` | PaymentApi |
| `ProviderStub__BaseUrl` | `http://provider-stub:8082` | PaymentApi |
| `WebhookReceiver__Url` | `http://webhook-receiver:8083` | ProviderStub |
| `Otel__Endpoint` | `http://otel-collector:4317` | All services |

---

## Additional Operations

**Kill switch — pause all payment processing:**
```bash
docker exec deploy-redis-1 redis-cli SET flags:payment_processing false
# Re-enable:
docker exec deploy-redis-1 redis-cli DEL flags:payment_processing
```

**Test idempotency — resubmit same key, expect 200 not 201:**
```bash
# Second call with same idempotencyKey returns 200 OK with the original response
curl -X POST http://localhost:8080/api/payments \
  -H "Content-Type: application/json" \
  -d '{"merchantId":"merchant_acme","idempotencyKey":"order-001","amount":99.99,"currency":"USD","customerEmail":"user@example.com","method":"CreditCard"}'
```

**Health checks:**
```bash
curl http://localhost:8080/health/live    # liveness — process running
curl http://localhost:8080/health/ready  # readiness — Postgres + Redis healthy
curl http://localhost:8080/health        # full — all checks
```

---

## AI Tools

This project was built with assistance from **Claude (Anthropic)** via Claude Code. AI was used to:

- Author the README and DESIGN.md
- Debug build and runtime errors encountered during `docker compose up` (missing packages, enum serialization, health check failures, CancellationToken parameter misuse)

All generated code was reviewed, corrected where needed, and validated by running the full stack end-to-end. The architecture decisions, resilience patterns, SLO definitions, and trade-off analysis reflect deliberate design choices rather than AI defaults.
