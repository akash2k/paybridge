using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.EntityFrameworkCore;
using PayBridge.PaymentApi.Infrastructure;
using PayBridge.Shared.Events;
using PayBridge.Shared.Models;

namespace PayBridge.PaymentApi.Services;

public class PaymentService
{
    private readonly AppDbContext _db;
    private readonly IFraudClient _fraud;
    private readonly IProviderClient _provider;
    private readonly IKafkaProducer _kafka;
    private readonly IIdempotencyService _idempotency;
    private readonly IFeatureFlags _flags;
    private readonly ILogger<PaymentService> _logger;

    // OTel instrumentation
    private static readonly ActivitySource _tracer = new("PayBridge.PaymentApi");
    private static readonly Meter _meter = new("PayBridge.PaymentApi");
    private static readonly Counter<long> _paymentsCreated = _meter.CreateCounter<long>(
        "payments_created_total", description: "Total payments created");
    private static readonly Counter<long> _fraudChecks = _meter.CreateCounter<long>(
        "fraud_checks_total", description: "Fraud check results");
    private static readonly Histogram<double> _processingDuration = _meter.CreateHistogram<double>(
        "payment_processing_duration_seconds", "s", "End-to-end payment processing time");

    public PaymentService(
        AppDbContext db,
        IFraudClient fraud,
        IProviderClient provider,
        IKafkaProducer kafka,
        IIdempotencyService idempotency,
        IFeatureFlags flags,
        ILogger<PaymentService> logger)
    {
        _db          = db;
        _fraud       = fraud;
        _provider    = provider;
        _kafka       = kafka;
        _idempotency = idempotency;
        _flags       = flags;
        _logger      = logger;
    }

    public async Task<(PaymentResponse response, bool wasCached)> CreateAsync(
        CreatePaymentRequest req,
        CancellationToken ct = default)
    {
        using var activity = _tracer.StartActivity("payment.create", ActivityKind.Internal);
        activity?.SetTag("payment.merchant_id",  req.MerchantId);
        activity?.SetTag("payment.currency",     req.Currency);
        activity?.SetTag("payment.method",       req.Method.ToString());
        activity?.SetTag("payment.amount",       req.Amount);

        var sw = Stopwatch.StartNew();

        // Kill switch check
        if (!await _flags.IsPaymentProcessingEnabledAsync())
        {
            _logger.LogWarning("Payment processing disabled via feature flag for merchant {MerchantId}", req.MerchantId);
            throw new InvalidOperationException("Payment processing is temporarily disabled");
        }

        // Idempotency check
        var idempotencyKey = $"idem:{req.MerchantId}:{req.IdempotencyKey}";
        var cached = await _idempotency.GetAsync<PaymentResponse>(idempotencyKey, ct);
        if (cached != null)
        {
            activity?.SetTag("payment.idempotency_hit", true);
            _logger.LogInformation("Idempotency cache hit for key {Key}", req.IdempotencyKey);
            return (cached, true);
        }

        // Create payment record
        var payment = new Payment
        {
            Id             = Guid.NewGuid(),
            MerchantId     = req.MerchantId,
            TenantId       = ResolveTenantId(req.MerchantId),
            IdempotencyKey = req.IdempotencyKey,
            Amount         = req.Amount,
            Currency       = req.Currency,
            Method         = req.Method,
            Status         = PaymentStatus.FraudChecking,
            CreatedAt      = DateTime.UtcNow
        };

        activity?.SetTag("payment.id", payment.Id.ToString());
        _db.Payments.Add(payment);
        await _db.SaveChangesAsync(ct);

        // Store trace context for webhook linking
        var traceId = Activity.Current?.TraceId.ToString() ?? string.Empty;
        await _idempotency.SetTraceAsync(payment.Id, traceId, ct);

        // Fraud check via gRPC
        using (var fraudActivity = _tracer.StartActivity("payment.fraud_check"))
        {
            fraudActivity?.SetTag("payment.id", payment.Id.ToString());
            var fraudResult = await _fraud.CheckAsync(payment, req.CustomerEmail, ct);

            _fraudChecks.Add(1,
                new KeyValuePair<string, object?>("result", fraudResult.Approved ? "approved" : "rejected"),
                new KeyValuePair<string, object?>("risk_bucket", GetRiskBucket(fraudResult.RiskScore)));

            fraudActivity?.SetTag("fraud.risk_score", fraudResult.RiskScore);
            fraudActivity?.SetTag("fraud.approved", fraudResult.Approved);

            if (!fraudResult.Approved)
            {
                payment.Status = PaymentStatus.Failed;
                payment.FailureReason = $"fraud_rejected:{fraudResult.Reason}";
                await _db.SaveChangesAsync(ct);

                _paymentsCreated.Add(1,
                    new KeyValuePair<string, object?>("status", "fraud_rejected"),
                    new KeyValuePair<string, object?>("method", req.Method.ToString()));

                _logger.LogWarning(
                    "Payment {PaymentId} rejected by fraud check: risk={RiskScore} reason={Reason}",
                    payment.Id, fraudResult.RiskScore, fraudResult.Reason);

                var rejectedResponse = new PaymentResponse(
                    payment.Id, PaymentStatus.Failed, null, payment.FailureReason, payment.CreatedAt);
                await _idempotency.SetAsync(idempotencyKey, rejectedResponse, ct: ct);
                return (rejectedResponse, false);
            }
        }

        // Submit to provider
        payment.Status = PaymentStatus.Submitted;
        await _db.SaveChangesAsync(ct);

        using (var providerActivity = _tracer.StartActivity("payment.provider_submit"))
        {
            providerActivity?.SetTag("payment.id", payment.Id.ToString());
            var providerResult = await _provider.SubmitAsync(payment, ct);
            providerActivity?.SetTag("provider.accepted", providerResult.Accepted);

            if (!providerResult.Accepted)
            {
                payment.Status = PaymentStatus.Failed;
                payment.FailureReason = providerResult.ErrorMessage;
                await _db.SaveChangesAsync(ct);

                _logger.LogError("Provider rejected payment {PaymentId}: {Error}",
                    payment.Id, providerResult.ErrorMessage);
            }
            else
            {
                payment.ProviderTransactionId = providerResult.ProviderTransactionId;
                await _db.SaveChangesAsync(ct);

                _logger.LogInformation(
                    "Payment {PaymentId} submitted to provider, providerTxId={ProviderTxId}",
                    payment.Id, payment.ProviderTransactionId);
            }
        }

        // Publish PaymentInitiated event
        await _kafka.PublishAsync("payment-events", new PaymentEvent(
            payment.Id,
            payment.MerchantId,
            payment.TenantId,
            "PaymentInitiated",
            payment.Amount,
            payment.Currency,
            payment.ProviderTransactionId,
            null,
            DateTime.UtcNow), ct);

        sw.Stop();
        _processingDuration.Record(sw.Elapsed.TotalSeconds,
            new KeyValuePair<string, object?>("stage", "full_create"));

        _paymentsCreated.Add(1,
            new KeyValuePair<string, object?>("status", payment.Status.ToString()),
            new KeyValuePair<string, object?>("method", req.Method.ToString()));

        var result = new PaymentResponse(
            payment.Id,
            payment.Status,
            payment.ProviderTransactionId,
            payment.FailureReason,
            payment.CreatedAt);

        await _idempotency.SetAsync(idempotencyKey, result, ct: ct);
        return (result, false);
    }

    public async Task<PaymentResponse?> GetAsync(Guid paymentId, CancellationToken ct = default)
    {
        var payment = await _db.Payments.AsNoTracking()
            .FirstOrDefaultAsync(p => p.Id == paymentId, ct);

        if (payment is null) return null;

        return new PaymentResponse(
            payment.Id,
            payment.Status,
            payment.ProviderTransactionId,
            payment.FailureReason,
            payment.CreatedAt);
    }

    private static string ResolveTenantId(string merchantId) =>
        // In production this would look up tenant from merchant config
        $"tenant_{merchantId.Split('_').FirstOrDefault() ?? "default"}";

    private static string GetRiskBucket(double score) => score switch
    {
        < 0.3 => "low",
        < 0.6 => "medium",
        < 0.8 => "high",
        _     => "critical"
    };
}
