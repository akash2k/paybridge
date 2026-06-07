using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using PayBridge.Shared.Events;
using PayBridge.Shared.Models;

namespace PayBridge.PaymentApi.Infrastructure;

public interface IKafkaProducer
{
    Task PublishAsync(string topic, PaymentEvent evt, CancellationToken ct = default);
}

public class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly AppDbContext _db;
    private readonly ILogger<KafkaProducer> _logger;
    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    public KafkaProducer(IConfiguration config, AppDbContext db, ILogger<KafkaProducer> logger)
    {
        _db = db;
        _logger = logger;
        var kafkaConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "kafka:9092",
            Acks = Acks.Leader,
            MessageTimeoutMs = 5000
        };
        _producer = new ProducerBuilder<string, string>(kafkaConfig).Build();
    }

    public async Task PublishAsync(string topic, PaymentEvent evt, CancellationToken ct = default)
    {
        var payload = JsonSerializer.Serialize(evt);
        var message = new Message<string, string>
        {
            Key = evt.PaymentId.ToString(),
            Value = payload,
            Headers = new Headers()
        };

        // Inject W3C trace context into Kafka headers
        Propagator.Inject(
            new PropagationContext(Activity.Current?.Context ?? default, Baggage.Current),
            message.Headers,
            (headers, key, value) => headers.Add(key, System.Text.Encoding.UTF8.GetBytes(value)));

        try
        {
            await _producer.ProduceAsync(topic, message, ct);
            _logger.LogInformation(
                "Published {EventType} for payment {PaymentId} to topic {Topic}",
                evt.EventType, evt.PaymentId, topic);
        }
        catch (KafkaException ex)
        {
            // Outbox fallback — payment must not fail because Kafka is down
            _logger.LogWarning(ex, "Kafka unavailable — writing event to outbox for payment {PaymentId}", evt.PaymentId);

            var traceContext = Activity.Current is { } a
                ? $"{a.TraceId}:{a.SpanId}"
                : null;

            _db.OutboxEvents.Add(new OutboxEvent
            {
                Topic = topic,
                Payload = payload,
                TraceContext = traceContext,
                CreatedAt = DateTime.UtcNow,
                RetryCount = 0
            });
            await _db.SaveChangesAsync(ct);
        }
    }

    public void Dispose() => _producer.Dispose();
}
