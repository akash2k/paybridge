using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using PayBridge.SettlementConsumer.Infrastructure;
using PayBridge.Shared.Events;
using PayBridge.Shared.Models;

namespace PayBridge.SettlementConsumer.Workers;

public class SettlementWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _config;
    private readonly ILogger<SettlementWorker> _logger;

    private static readonly ActivitySource Tracer = new("PayBridge.SettlementConsumer");
    private static readonly Meter Meter = new("PayBridge.SettlementConsumer");
    private static readonly Counter<long> SettlementsTotal =
        Meter.CreateCounter<long>("settlement_records_persisted_total");
    private static readonly Histogram<double> ProcessingDuration =
        Meter.CreateHistogram<double>("settlement_processing_duration_seconds", "s");
    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    public SettlementWorker(IServiceScopeFactory scopeFactory, IConfiguration config, ILogger<SettlementWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Settlement consumer starting");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers    = _config["Kafka:BootstrapServers"] ?? "kafka:9092",
            GroupId             = "settlement-consumer",
            AutoOffsetReset     = AutoOffsetReset.Earliest,
            EnableAutoCommit    = false,    // manual commit after successful processing
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe("payment-events");

        _logger.LogInformation("Subscribed to payment-events topic");

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result = null;
            try
            {
                result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is null) continue;

                await ProcessMessageAsync(result, stoppingToken);
                consumer.Commit(result);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Kafka message offset={Offset}",
                    result?.Offset.Value);
                // Don't commit — message will be re-delivered
                await Task.Delay(1000, stoppingToken);
            }
        }

        consumer.Close();
    }

    private async Task ProcessMessageAsync(
        ConsumeResult<string, string> result,
        CancellationToken ct)
    {
        // Extract W3C trace context from Kafka message headers
        var parentContext = Propagator.Extract(
            default,
            result.Message.Headers,
            (headers, key) =>
            {
                var header = headers.FirstOrDefault(h => h.Key == key);
                return header is null
                    ? Enumerable.Empty<string>()
                    : new[] { Encoding.UTF8.GetString(header.GetValueBytes()) };
            });

        Baggage.Current = parentContext.Baggage;

        using var activity = Tracer.StartActivity(
            "settlement.process",
            ActivityKind.Consumer,
            parentContext.ActivityContext);

        var sw = System.Diagnostics.Stopwatch.StartNew();

        var evt = JsonSerializer.Deserialize<PaymentEvent>(result.Message.Value);
        if (evt is null)
        {
            _logger.LogWarning("Failed to deserialize Kafka message at offset {Offset}", result.Offset.Value);
            return;
        }

        // Only process terminal events
        if (evt.EventType is not ("PaymentCompleted" or "PaymentFailed"))
        {
            _logger.LogDebug("Skipping non-terminal event {EventType} for payment {PaymentId}",
                evt.EventType, evt.PaymentId);
            return;
        }

        activity?.SetTag("payment.id",         evt.PaymentId.ToString());
        activity?.SetTag("event.type",         evt.EventType);
        activity?.SetTag("kafka.offset",       result.Offset.Value);
        activity?.SetTag("kafka.partition",    result.Partition.Value);

        _logger.LogInformation(
            "Processing {EventType} for payment {PaymentId} offset={Offset}",
            evt.EventType, evt.PaymentId, result.Offset.Value);

        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        // Enrich from payment record if event has missing data (e.g. from webhook path)
        var payment = await db.Payments.AsNoTracking()
            .FirstOrDefaultAsync(p => p.Id == evt.PaymentId, ct);

        var finalStatus = evt.EventType == "PaymentCompleted"
            ? PaymentStatus.Completed
            : PaymentStatus.Failed;

        // Idempotent upsert — ON CONFLICT DO NOTHING prevents double-settlement
        var rowsAffected = await db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO "SettlementRecords"
                ("PaymentId","MerchantId","TenantId","Amount","Currency",
                 "FinalStatus","ProviderTransactionId","EventTimestamp","PersistedAt")
            VALUES
                ({0},{1},{2},{3},{4},{5},{6},{7},{8})
            ON CONFLICT ("PaymentId") DO NOTHING
            """,
            evt.PaymentId,
            payment?.MerchantId ?? evt.MerchantId,
            payment?.TenantId   ?? evt.TenantId,
            payment?.Amount     ?? evt.Amount,
            payment?.Currency   ?? evt.Currency,
            finalStatus.ToString(),
            evt.ProviderTransactionId,
            evt.Timestamp,
            DateTime.UtcNow,
            ct);

        sw.Stop();
        ProcessingDuration.Record(sw.Elapsed.TotalSeconds);

        if (rowsAffected > 0)
        {
            SettlementsTotal.Add(1,
                new KeyValuePair<string, object?>("status", finalStatus.ToString()));
            _logger.LogInformation(
                "Settlement persisted for payment {PaymentId} status={Status}",
                evt.PaymentId, finalStatus);
        }
        else
        {
            _logger.LogInformation(
                "Settlement already exists for payment {PaymentId} (idempotent skip)",
                evt.PaymentId);
        }

        // Update payment status in payments table
        if (payment is not null)
        {
            await db.Payments
                .Where(p => p.Id == evt.PaymentId)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(p => p.Status, finalStatus)
                    .SetProperty(p => p.CompletedAt, DateTime.UtcNow)
                    .SetProperty(p => p.ProviderTransactionId,
                        p => evt.ProviderTransactionId ?? p.ProviderTransactionId),
                ct);
        }
    }
}
