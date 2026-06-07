using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Confluent.Kafka;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;
using PayBridge.Shared.Events;
using Serilog;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();
builder.Host.UseSerilog();

var redisConn = builder.Configuration["Redis:ConnectionString"] ?? "redis:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(ConnectionMultiplexer.Connect(redisConn));

var otelEndpoint = builder.Configuration["Otel:Endpoint"] ?? "http://otel-collector:4317";
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("PayBridge.WebhookReceiver"))
    .WithTracing(t => t
        .AddSource("PayBridge.WebhookReceiver")
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter(o => o.Endpoint = new Uri(otelEndpoint)))
    .WithMetrics(m => m
        .AddMeter("PayBridge.WebhookReceiver")
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter(o => o.Endpoint = new Uri(otelEndpoint)));

builder.Services.AddHealthChecks()
    .AddRedis(redisConn, name: "redis", tags: ["critical"]);

var app = builder.Build();

// ─── Instrumentation setup ───────────────────────────────────────────────────
var tracer = new ActivitySource("PayBridge.WebhookReceiver");
var meter  = new Meter("PayBridge.WebhookReceiver");
var webhooksReceived = meter.CreateCounter<long>("webhooks_received_total");

// Kafka producer
var kafkaConfig = new ProducerConfig
{
    BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "kafka:9092"
};
var kafkaProducer = new ProducerBuilder<string, string>(kafkaConfig).Build();
var logger = app.Logger;

// ─── Webhook endpoint ────────────────────────────────────────────────────────
app.MapPost("/webhooks/provider", async (
    ProviderWebhookCallback callback,
    IConnectionMultiplexer redis) =>
{
    var db = redis.GetDatabase();

    // Extract paybridge_payment_id from metadata
    if (!callback.Metadata.TryGetValue("paybridge_payment_id", out var paymentIdStr)
        || !Guid.TryParse(paymentIdStr, out var paymentId))
    {
        logger.LogWarning("Webhook missing paybridge_payment_id");
        return Results.BadRequest("Missing paybridge_payment_id in metadata");
    }

    // Deduplication — SET NX with 7-day TTL
    var dedupKey = $"webhook:processed:{callback.ProviderTransactionId}";
    var isNew = await db.StringSetAsync(dedupKey, "1", TimeSpan.FromDays(7), When.NotExists);
    if (!isNew)
    {
        webhooksReceived.Add(1, new KeyValuePair<string, object?>("status", "duplicate"));
        logger.LogInformation("Duplicate webhook ignored for providerTxId={TxId}", callback.ProviderTransactionId);
        return Results.Ok(new { status = "already_processed" });
    }

    // ── Trace linking ────────────────────────────────────────────────────────
    // This webhook is a brand-new HTTP request with no trace parent.
    // We link back to the original payment trace using ActivityLink.
    var links = new List<ActivityLink>();
    var originalTraceId = await db.StringGetAsync($"trace:{paymentId}");

    if (!originalTraceId.IsNullOrEmpty)
    {
        // Parse stored "traceId:spanId" string
        var parts = originalTraceId.ToString().Split(':');
        if (parts.Length == 2
            && ActivityTraceId.TryParse(parts[0], out var traceId)
            && ActivitySpanId.TryParse(parts[1], out var spanId))
        {
            var linkedCtx = new ActivityContext(traceId, spanId, ActivityTraceFlags.Recorded);
            links.Add(new ActivityLink(linkedCtx));
        }
    }

    using var activity = tracer.StartActivity(
        "webhook.provider_callback",
        ActivityKind.Server,
        parentContext: default,   // this is a new trace
        links: links);            // linked to the original payment trace

    activity?.SetTag("payment.id",              paymentId.ToString());
    activity?.SetTag("provider.transaction_id", callback.ProviderTransactionId);
    activity?.SetTag("provider.status",         callback.Status);

    // Determine event type
    var isSuccess  = callback.Status.Equals("SUCCESS", StringComparison.OrdinalIgnoreCase);
    var eventType  = isSuccess ? "PaymentCompleted" : "PaymentFailed";

    logger.LogInformation(
        "Webhook received for payment {PaymentId} status={Status} providerTxId={TxId}",
        paymentId, callback.Status, callback.ProviderTransactionId);

    // Publish to Kafka — inject trace context into headers
    var evt = new PaymentEvent(
        paymentId,
        MerchantId:          "unknown",  // settlement consumer will look up from DB
        TenantId:            "unknown",
        EventType:           eventType,
        Amount:              0,           // settlement consumer enriches from DB
        Currency:            "USD",
        ProviderTransactionId: callback.ProviderTransactionId,
        FailureReason:       isSuccess ? null : $"provider_failure:{callback.Status}",
        Timestamp:           callback.Timestamp);

    var payload = JsonSerializer.Serialize(evt);
    var message = new Message<string, string>
    {
        Key   = paymentId.ToString(),
        Value = payload,
        Headers = new Headers
        {
            { "X-Payment-Id", System.Text.Encoding.UTF8.GetBytes(paymentId.ToString()) }
        }
    };

    await kafkaProducer.ProduceAsync("payment-events", message);

    webhooksReceived.Add(1, new KeyValuePair<string, object?>("status", isSuccess ? "success" : "failure"));

    return Results.Ok(new { status = "accepted", eventType });
});

app.MapHealthChecks("/health");
app.Run();
