using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using PayBridge.PaymentApi.Infrastructure;
using PayBridge.Shared.Events;

namespace PayBridge.PaymentApi.Services;

public class OutboxWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OutboxWorker> _logger;
    private readonly IProducer<string, string> _producer;
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(5);

    public OutboxWorker(IServiceScopeFactory scopeFactory, IConfiguration config, ILogger<OutboxWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "kafka:9092"
        }).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox worker started");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessOutboxAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outbox worker error");
            }
            await Task.Delay(PollInterval, stoppingToken);
        }
    }

    private async Task ProcessOutboxAsync(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var pending = await db.OutboxEvents
            .Where(o => o.ProcessedAt == null && o.RetryCount < 5)
            .OrderBy(o => o.CreatedAt)
            .Take(50)
            .ToListAsync(ct);

        foreach (var outbox in pending)
        {
            try
            {
                var evt = JsonSerializer.Deserialize<PaymentEvent>(outbox.Payload)!;
                await _producer.ProduceAsync(outbox.Topic, new Message<string, string>
                {
                    Key   = evt.PaymentId.ToString(),
                    Value = outbox.Payload
                }, ct);

                outbox.ProcessedAt = DateTime.UtcNow;
                _logger.LogInformation("Outbox event {Id} published to {Topic}", outbox.Id, outbox.Topic);
            }
            catch (KafkaException ex)
            {
                outbox.RetryCount++;
                _logger.LogWarning(ex, "Outbox retry {Retry} failed for event {Id}", outbox.RetryCount, outbox.Id);
            }
        }

        await db.SaveChangesAsync(ct);
    }

    public override void Dispose()
    {
        _producer.Dispose();
        base.Dispose();
    }
}
